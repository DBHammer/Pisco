import requests
import json
import time
import logging
import random
from collections import OrderedDict

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class LLMConnector:
    """
    Manages communication with an LLM, including a multi-stage expert consultation
    protocol for complex comparisons and tracking of API token usage.
    """
    def __init__(self, llm_url="http://localhost:11434/api/generate", model="qwen2.5:7b"):
        self.llm_url = llm_url
        self.model = model
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # --- Token Usage Statistics ---
        self.prompt_tokens = 0
        self.completion_tokens = 0
        
        self.roles = ['Consistent Read', 'Mutual Exclusion', 'Serialization Certifier', 'First Updater Wins']
        
        self.instruction_variants = [
            "You must select either '1' or '2'. Respond with a JSON object containing two keys: 'decision', which must be a string ('1' or '2'), and 'confidence', a float from 0.0 to 1.0 indicating your certainty. Provide no other text.",
            "Your output must be a single JSON object. This object should have a 'decision' key (with value '1' or '2') and a 'confidence' key (a float between 0.0 and 1.0). Do not include explanations or any other characters.",
            "Strictly output a JSON object. It must contain: a 'decision' field (string '1' or '2') and a 'confidence' field (float 0.0-1.0). Nothing else.",
        ]

        self.knowledge = {
            'Consistent Read': {
                'description': "Provides a consistent view of the database at a specific time point",
                'mechanism': "MVCC maintains multiple physical versions of each record",
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
                'purpose': "Eliminates lost update anomaly"
            },
            'Serialization Certifier': {
                'description': "Guarantees conflict-serializable transaction execution",
                'purpose': "Eliminates serialization anomaly in CCPs",
                'variants': {'SSI': "Detects write skew anomalies", 'TO': "Uses timestamp ordering"}
            }
        }

    # --- NEW: Methods for Token Usage ---
    def get_token_usage(self):
        """
        Returns a dictionary with the cumulative token usage.

        Returns:
            dict: A dictionary with 'prompt_tokens', 'completion_tokens', and 'total_tokens'.
        """
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.prompt_tokens + self.completion_tokens
        }

    def reset_token_usage(self):
        """Resets the token usage counters to zero."""
        self.logger.info("Resetting token usage counters.")
        self.prompt_tokens = 0
        self.completion_tokens = 0

    def _get_expert_prompt(self):
        prompt_lines = [
            "You are a database concurrency control expert with deep knowledge of:",
            ", ".join(self.roles) + ".\n"
            "Your expertise includes:"
        ]
        
        for role, details in self.knowledge.items():
            prompt_lines.append(f"\n- {role}:")
            prompt_lines.append(f"  * {details['description']}")
            prompt_lines.append(f"  * Primary purpose: {details['purpose']}")
            if 'mechanism' in details:
                prompt_lines.append(f"  * Key mechanism: {details['mechanism']}")
        
        return "\n".join(prompt_lines)

    def _query_llm(self, prompt, max_tokens=100):
        data = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "keep_alive": '10h',
            "options": {
                "num_predict": max_tokens
            }
        }
        try:
            response = requests.post(self.llm_url, json=data, timeout=20)
            response.raise_for_status()

            # Parse the entire response to get content and token counts
            full_response = json.loads(response.text)
            
            # --- MODIFIED: Update token counts ---
            prompt_tokens = full_response.get("prompt_eval_count", 0)
            completion_tokens = full_response.get("eval_count", 0)
            self.prompt_tokens += prompt_tokens
            self.completion_tokens += completion_tokens
            self.logger.debug(
                f"Tokens used: Prompt={prompt_tokens}, Completion={completion_tokens}. "
                f"Cumulative Total: {self.get_token_usage()['total_tokens']}"
            )

            llm_response_str = full_response.get("response", "")
            if not llm_response_str:
                self.logger.warning("LLM returned an empty response string.")
                return None
            output_json = json.loads(llm_response_str)
            return output_json
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"LLM request error: {e}")
            return None
        except json.JSONDecodeError as e:
            # This can happen if response.text is not valid JSON, or if llm_response_str is not.
            raw_text = response.text if 'response' in locals() else 'N/A'
            self.logger.warning(f"Failed to parse JSON response from LLM: {e}")
            self.logger.debug(f"Raw response text from LLM: {raw_text}")
            return None
        except Exception as e:
            self.logger.error(f"An unexpected error occurred in _query_llm: {e}")
            return None

    def _shuffle_dict(self, d):
        items = list(d.items())
        random.shuffle(items)
        return OrderedDict(items)

    def _format_prompt(self, role, report1, report2, case, require_cot=False, explanation=None):
        instruction = random.choice(self.instruction_variants)
        report1_shuffled = self._shuffle_dict(report1.features)
        report2_shuffled = self._shuffle_dict(report2.features)
        
        explanation_prompt = ""
        if explanation:
            explanation_prompt = (
                "Before making your final decision, please review the analyses from other experts in the previous round:\n"
                f"{explanation}\n"
                "Now, provide your own final, reasoned judgment.\n\n"
            )

        cot_prompt = "Think step-by-step: " if require_cot else ""

        prompt = (
            f"{self._get_expert_prompt()}\n\n"
            f"{explanation_prompt}"
            f"You are now acting as a '{role}' expert.\n"
            f"Report 1: {json.dumps(report1_shuffled)}\n"
            f"Report 2: {json.dumps(report2_shuffled)}\n"
            f"Case: {json.dumps(case)}\n\n"
            "TASK: Which report is more likely related to the same root cause as the case and is in the same DBMS?\n"
            f"{cot_prompt}"
            f"INSTRUCTION: {instruction}"
        )
        return prompt

    def _parse_response(self, response_dict):
        if not isinstance(response_dict, dict):
            self.logger.warning(f"Invalid response format: Expected dict, got {type(response_dict)}")
            return None, 0.0

        decision_val = response_dict.get('decision')
        decision = str(decision_val) if decision_val is not None else None
        confidence = response_dict.get('confidence')

        if decision not in ['1', '2']:
            self.logger.warning(f"Invalid 'decision' value in LLM response: {decision_val} (type: {type(decision_val)})")
            return None, 0.0
        
        try:
            confidence = float(confidence)
            if not (0.0 <= confidence <= 1.0):
                self.logger.warning(f"Confidence score {confidence} out of range [0.0, 1.0]. Clamping.")
                confidence = max(0.0, min(1.0, confidence))
        except (ValueError, TypeError):
            self.logger.warning(f"Invalid 'confidence' value in LLM response: {confidence}. Using 0.5 as default.")
            return decision, 0.5

        return decision, confidence

    def _collect_expert_opinions(self, report1, report2, case, use_cot=False, explanations_str=None):
        scores = []
        raw_responses = []
        max_tokens = 1500 if use_cot else 100

        for role in self.roles:
            prompt = self._format_prompt(role, report1, report2, case, require_cot=use_cot, explanation=explanations_str)
            response_dict = self._query_llm(prompt, max_tokens=max_tokens)
            decision, confidence = self._parse_response(response_dict)

            if decision in ['1', '2']:
                scores.append(confidence if decision == '1' else -confidence)
                raw_responses.append((role, response_dict)) 
            else:
                self.logger.info(f"Skipped expert {role} due to invalid response: {response_dict}")
                raw_responses.append((role, None))
        
        return scores, raw_responses

    def _check_consensus(self, scores, threshold=0.5):
        if not scores:
            return False
        
        num_valid_opinions = len(scores)
        num_experts = len(self.roles)

        if num_valid_opinions < num_experts * threshold:
            self.logger.info(f"Not enough valid opinions ({num_valid_opinions}/{num_experts}) to reach consensus threshold of {threshold*100}%.")
            return False

        all_positive = all(s > -threshold for s in scores)
        all_negative = all(s < +threshold for s in scores)
        
        return all_positive or all_negative

    def _aggregate_scores(self, scores):
        if not scores:
            return 0.0
        return max(scores, key=abs)

    def compare_by_experts(self, report1, report2, case, max_collaboration_rounds=3):
        # --- STAGE 1: Quick Poll ---
        self.logger.info("--- STAGE 1: Quick Poll ---")
        scores, _ = self._collect_expert_opinions(report1, report2, case, use_cot=False)
        
        if self._check_consensus(scores):
            final_score = self._aggregate_scores(scores)
            self.logger.info(f"Consensus reached in Stage 1. Final score: {final_score:.2f}")
            return final_score

        # --- STAGE 2: Detailed Analysis (CoT) ---
        self.logger.info("--- STAGE 2: No consensus. Requesting detailed analysis (CoT) ---")
        scores, raw_responses = self._collect_expert_opinions(report1, report2, case, use_cot=True)

        if self._check_consensus(scores):
            final_score = self._aggregate_scores(scores)
            self.logger.info(f"Consensus reached in Stage 2. Final score: {final_score:.2f}")
            return final_score

        # --- STAGE 3: Iterative Collaboration ---
        self.logger.info("--- STAGE 3: Still no consensus. Starting iterative collaboration ---")
        
        current_round = 0
        while current_round < max_collaboration_rounds:
            self.logger.info(f"--- Collaboration Round {current_round + 1}/{max_collaboration_rounds} ---")
            
            explanation_texts = []
            for role, resp in raw_responses:
                if resp:
                    explanation_texts.append(
                        f"- Expert '{role}' in the previous round decided: {resp.get('decision')} "
                        f"with confidence {resp.get('confidence')}"
                    )
            explanations_str = "\n".join(explanation_texts)
            
            scores, raw_responses = self._collect_expert_opinions(
                report1, report2, case, 
                use_cot=True,
                explanations_str=explanations_str
            )
            
            if self._check_consensus(scores):
                final_score = self._aggregate_scores(scores)
                self.logger.info(f"Consensus reached in Collaboration Round {current_round + 1}. Final score: {final_score:.2f}")
                return final_score
            
            current_round += 1
            if current_round < max_collaboration_rounds:
                self.logger.info("No consensus yet. Proceeding to the next collaboration round.")

        # --- Fallback: If no consensus after all rounds ---
        self.logger.warning(
            f"No consensus reached after {max_collaboration_rounds} rounds of collaboration. "
            "Forcing a decision based on the final round's average score."
        )
        
        if not scores:
            self.logger.error("No valid opinions in the final collaboration stage. Returning 0.")
            return 0.0
        
        final_collaborative_score = sum(scores) / len(scores)
        self.logger.info(f"Forced decision score (average of last round): {final_collaborative_score:.2f}")
        
        return final_collaborative_score

    def ask_LLM_to_select(self, report_ids, existing_result):
        report_ids_str = str(report_ids).replace('\'', '')
        results_str = str(json.dumps(existing_result)).replace('\"', '')
        statement = (f"Here are a set of compare results: {results_str}. "
                     f"And we have these bug reports: {report_ids_str}. "
                     "Please select two bug reports from the list for the next comparison. "
                     "Note you should not select already compared bug reports. "
                     "You should only answer the id of the reports, concatenated with a space, without any other words.")

        self.logger.info(f"Asking LLM to select next pair with prompt: {statement}")
        # This part does not use JSON mode, so it is a standard text-generation call.
        data = {"model": self.model, "prompt": statement, "stream": False, "keep_alive": '10h'}
        try:
            response = requests.post(self.llm_url, json=data, timeout=10)
            response.raise_for_status()
            
            # --- MODIFIED: Parse full response and update token counts ---
            full_response = json.loads(response.text)
            prompt_tokens = full_response.get("prompt_eval_count", 0)
            completion_tokens = full_response.get("eval_count", 0)
            self.prompt_tokens += prompt_tokens
            self.completion_tokens += completion_tokens
            self.logger.debug(
                f"Tokens used: Prompt={prompt_tokens}, Completion={completion_tokens}. "
                f"Cumulative Total: {self.get_token_usage()['total_tokens']}"
            )
            
            text_response = full_response.get("response", "").strip()
            if not text_response or len(text_response.split()) != 2:
                self.logger.warning(f"Could not parse LLM selection response: {text_response}")
                return None
            return text_response
        except Exception as e:
            self.logger.warning(f"LLM selection request error: {e}")
            return None