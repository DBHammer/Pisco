import requests
import json
import time
import logging
import random
# from transformers import AutoTokenizer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'  # 指定时间格式
)

class LLMConnector:
    # it is strange that other models cannot understand the output format
    def __init__(self, llm_url = "http://localhost:11434/api/generate", model = "qwen2.5:7b"):
        self.llm_url = llm_url
        self.model = model
        self.isStream = False
        self.FORBIDDEN_CHARS = ['"', '\\n', '\n', '\\']
        self.roles = ['Consistent Read','Mutual Exclusion','Serialization Certifier','First Updater Wins']
        self.logger = logging.getLogger(__name__)
        self.knowledge = {
            'Consistent Read': {
                'description': "Provides a consistent view of the database at a specific time point",
                'mechanism': "MVCC maintains multiple physical versions of each record",
                'levels': {
                    'transaction': "Consistent view at transaction beginning",
                    'statement': "Consistent view at operation beginning"
                },
                'purpose': "Eliminates read skew anomaly by showing a database snapshot"
            },
            'Mutual Exclusion': {
                'description': "Uses locking strategy for exclusive access to shared resources",
                'mechanism': "Two-phase locking (2PL) with distinct acquire/release phases",
                'purpose': "Generates serializable histories by detecting and delaying conflicts"
            },
            'First Updater Wins': {
                'description': "Prevents concurrent modifications of the same record",
                'mechanism': "Ensures serial order for transactions modifying same record",
                'purpose': "Eliminates lost update anomaly",
                'common_use': "Often combined with Consistent Read for snapshot isolation"
            },
            'Serialization Certifier': {
                'description': "Guarantees conflict-serializable transaction execution",
                'purpose': "Eliminates serialization anomaly in CCPs",
                'variants': {
                    'SSI': "Detects write skew anomalies",
                    'TO': "Uses timestamp ordering",
                    'OCC': "Uses conflict checking between transaction phases"
                }
            }
        }

    def _get_expert_prompt(self):
        prompt_lines = [
            "You are a database concurrency control expert with deep knowledge of:",
            ", ".join(self.roles) + ".\n",
            "Your expertise includes:"
        ]
        
        for role, details in self.knowledge.items():
            prompt_lines.append(f"\n- {role}:")
            prompt_lines.append(f"  * {details['description']}")
            prompt_lines.append(f"  * Primary purpose: {details['purpose']}")
            if 'mechanism' in details:
                prompt_lines.append(f"  * Key mechanism: {details['mechanism']}")
        
        
        return "\n".join(prompt_lines)

    # get response from LLM
    def _query_llm(self, prompt, max_tokens=500):
        data = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "keep_alive": '10h',
            "max_tokens": max_tokens
        }
        try:
            response = requests.post(self.llm_url, json=data, timeout=10)
            response.raise_for_status()
            output_json = json.loads(response.text)
            return output_json.get("response", "").strip().lower()
        except Exception as e:
            self.logger.warning(f"LLM request error: {e}")
            return None

    # remove forbidden characters
    def _parse_string(self, message:str):
        request = message
        # for char in self.FORBIDDEN_CHARS:
        #     request = request.replace(char, "")
        return request
    def _format_prompt(self, role, report1, report2, case, require_cot=False, explanation=None):

        prompt = (
            f"{self._get_expert_prompt()}\n"
            f"Report 1: {json.dumps(report1.features)}\n"
            f"Report 2: {json.dumps(report2.features)}\n"
            f"Case: {json.dumps(case)}\n"
            "Which report is more likely related to the same root cause as the case?\n"
            "You should only answer '1' or '2' with a relevance score from 0 to 1, indicating how relevant the option is to your mechanism. Concatenate with a space. No other words should be provided."
        )
        if require_cot:
            prompt = "Think step-by-step:" + prompt
        if explanation is not None:
            prompt = f"With explanation as {explanation}" + prompt
        return prompt
    def _parse_response(self, response):
        if not response:
            return None, 0.0
        print(response)
        parts = response.strip().split()
        if len(parts) < 2:
            return None, 0.0
        try:
            return parts[-2], float(parts[-1])
        except ValueError:
            return None, 0.0
    

    def ask_LLM_to_select(self, report_ids, existing_result):
        ret = 0
        
        report_ids = str(report_ids).replace('\'','')
        results = str(json.dumps(existing_result)).replace('\"','')
        statement = f"Here are a set of compare results: {results}. And we have these bug reports: {report_ids}. Please select two bug reports from the list for next comparison. Note you should not select compared bug reports. You should only answer the id of reports concating with a space without any other word."

        # statement = self._parse_string(statement)
        print(statement)
        response = self._query_llm(statement)
        if response == "Error in decoding response" or len(response.split()) != 2:
            return None
        
        return response

    def compare_by_experts(self, report1, report2, case):

        # Step 1: Collect judgments and confidences
        scores = []
        decisions = []
        explanations = []
        # for _ in range(3):
        for role in self.roles:
            prompt = self._format_prompt(role, report1, report2, case)
            response = self._query_llm(prompt)
            decision, confidence = self._parse_response(response)

            if decision in ['1', '2']:
                decisions.append((decision, confidence, role))
                scores.append(confidence if decision == '1' else -confidence)
            else:
                self.logger.info(f"Skipped expert {role} due to invalid response: {response}")

            # Step 2: Consensus check
            if len(scores) == 0:
                continue
        
        final_score = max(scores) - min(scores)
        print(scores, final_score)
        if abs(final_score) < 1:  # Threshold for consensus
            return max(scores)

        # Step 3: Chain-of-Thought reasoning
        scores = []
        self.logger.info("No consensus. Requesting CoT from experts.")
        for role in self.roles:
            prompt = self._format_prompt(role, report1, report2, case, require_cot=True)
            response = self._query_llm(prompt, max_tokens=1500)
            decision, confidence = self._parse_response(response)

            if decision in ['1', '2']:
                decisions.append((decision, confidence, role))
                scores.append(confidence if decision == '1' else -confidence)
            else:
                self.logger.info(f"Skipped expert {role} due to invalid response: {response}")
            explanations.append((role, response))
            scores.append(confidence if decision == '1' else -confidence)
        final_score = max(scores) - min(scores)
        if abs(final_score) < 1:  # Threshold for consensus
            return max(scores)

        # Step 4: Simulated collaboration (for simplicity, aggregate again)
        # You could replace this with real interaction logic
        collab_score = 0
        for role in self.roles:
            prompt = self._format_prompt(role, report1, report2, case, require_cot=True, explanation=explanations)
            response = self._query_llm(prompt, max_tokens=1500)
            decision, confidence = self._parse_response(response)
            collab_score += confidence if decision == '1' else -confidence

        return collab_score
