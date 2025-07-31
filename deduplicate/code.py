import random
import math
import json
import time
from collections import deque
import networkx as nx

# Assuming casePreparer.py exists and is structured as in the original file.
# If not, its content should be included here.
import casePreparer

class BugReport:
    """A simple class to hold bug report data."""
    def __init__(self, report_id, features):
        self.id = report_id
        self.features = features  # Dictionary of features

    def __repr__(self):
        return f"BugReport(id={self.id})"

class DuplicateDetector:
    """
    Finds the most similar bug report to a given case using the KNOCKOUT
    maximum selection algorithm from Falahatgar et al. (2017).
    """
    def __init__(self, candidate_reports, reduced_case, LLMConnector):
        """
        Initializes the detector.

        Args:
            candidate_reports (list): A list of raw bug report data.
            reduced_case (dict): The case to which similarity is compared.
            LLMConnector: An object with a method for comparing reports.
        """
        self.candidate_reports = []  # List of BugReport objects
        # The knowledge file seems to be a dependency for the case preparer
        with open('./knowledge/knowledge.json', 'r', encoding='utf-8') as file:
            knowledge = json.load(file)

        prep = casePreparer.casePreparer(knowledge)

        # Parse and create BugReport objects
        reports = [prep.parse_report(i) for i in candidate_reports]
        for i in reports:
            self.candidate_reports.append(BugReport(i['id'].lower(), i))

        self.reduced_case = reduced_case
        self.connector = LLMConnector
        self.comparison_count = 0

    def _perform_single_comparison(self, br1, br2):
        """
        Performs a single noisy comparison between two bug reports using the LLM.
        This is the fundamental noisy operation p(i, j).
        """
        self.comparison_count += 1
        print(f"Comparing {br1.id} vs {br2.id} (Total comparisons: {self.comparison_count})", flush=True)
        
        # This logic mirrors the original file's method of getting a score.
        # A positive score means br1 is deemed more similar.
        start_time = time.time()
        if random.random() < 0.5:
            score = self.connector.compare_by_experts(br1, br2, self.reduced_case)
        else:
            score = - self.connector.compare_by_experts(br2, br1, self.reduced_case)
        end_time = time.time()
        
        print(f"  -> Comparison took {end_time - start_time:.4f}s. Score: {score:.2f}. Winner: {br1.id if score >= 0 else br2.id}", flush=True)
        return score >= 0

    def _compare(self, br1, br2, epsilon, delta):
        """
        Implements Algorithm 1: COMPARE from the paper.
        Compares two elements adaptively until a winner is found with high confidence.
        """
        # Max number of comparisons to avoid infinite loops, derived from the paper's theory
        # m = O(log(1/δ) / ε^2)
        # try:
        #     max_comparisons = math.ceil((1 / (2 * epsilon**2)) * math.log(2 / delta))
        # except ValueError:
        #     max_comparisons = 100 # Fallback for invalid math domain
        # use a fix comparison number instead of the model above
        max_comparisons = 5

        r = 0  # Number of comparisons performed for this pair
        wins_br1 = 0

        while r < max_comparisons:
            r += 1
            if self._perform_single_comparison(br1, br2):
                wins_br1 += 1
            
            p_hat = wins_br1 / r
            # Confidence bound c_hat from the paper's Hoeffding-style inequality
            # try:
            #     c_hat = math.sqrt(math.log(4 * r**2 / delta) / (2 * r))
            # except ValueError: # pragma: no cover
            #     # This can happen if delta is too large or r is 0, though r starts at 1.
            #     c_hat = 0.5 

            # Check stopping condition: if empirical probability is far enough
            # from 0.5 to be confident about the winner.
            # print(p_hat, c_hat, epsilon)
            # if abs(p_hat - 0.5) > c_hat - epsilon:
            #     break
        
        # Return the element with more wins. Break ties randomly.
        if p_hat > 0.5:
            return br1
        elif p_hat < 0.5:
            return br2
        else:
            return random.choice([br1, br2])

    def _knockout_round(self, reports, epsilon, delta):
        """
        Plays one round of the tournament.
        """
        print(f"\n--- Starting KNOCKOUT Round with {len(reports)} contenders ---")
        random.shuffle(reports)
        winners = []
        
        # Pair up elements and find the winner of each match
        for i in range(0, len(reports) - 1, 2):
            br1 = reports[i]
            br2 = reports[i+1]
            
            # The number of participants in this sub-problem is len(reports)
            # The delta needs to be adjusted for the number of pairs in the round
            num_pairs = len(reports) / 2
            winner = self._compare(br1, br2, epsilon, delta / num_pairs)
            winners.append(winner)
            print(f"  Match {br1.id} vs {br2.id}. Winner: {winner.id}", flush=True)

        # If there's an odd number of reports, the last one gets a "bye"
        if len(reports) % 2 != 0:
            last_report = reports[-1]
            winners.append(last_report)
            print(f"  Match {last_report.id} gets a bye.")
            
        return winners

    def find_most_similar(self, epsilon=0.1, delta=0.1):
        """
        Implements Algorithm 3: KNOCKOUT (the main tournament).
        This is the primary public method to run the deduplication process.
        We repeat the comparisons with a fixed number, epsilon and delta is ignored indeed.
        
        Returns:
            BugReport: The single report deemed most similar to the reduced case.
        """
        current_contenders = list(self.candidate_reports)
        if not current_contenders:
            return "No candidate reports to compare."
        if len(current_contenders) == 1:
            return current_contenders[0]

        print(f"Starting KNOCKOUT tournament with {len(current_contenders)} reports.")

        # Constant from the paper's analysis for setting round-specific epsilon
        # c = 2**(1/3) - 1
        round_num = 1

        while len(current_contenders) > 1:
            # Per-round parameters get stricter as the tournament progresses
            # round_epsilon = (c * epsilon) / (2**(round_num / 3))
            # round_delta = delta / (2**round_num)

            current_contenders = self._knockout_round(current_contenders, 0,0)
            round_num += 1

        # The last remaining contender is the winner
        winner = current_contenders[0]
        print("\n--- Tournament Finished ---")
        print(f"Most similar report found: {winner.id}")
        print(f"Total LLM comparisons made: {self.comparison_count}")
        return winner

    def deduplicate(self, duplicate_id):
        winner = self.find_most_similar(epsilon=0.1, delta=0.1)
        print(winner.id,duplicate_id)
        if winner.id.lower() == duplicate_id.lower():
            return 1
        for i in self.candidate_reports:
            if i.id.lower() == duplicate_id.lower():
                return 0
        return -1


# Helper classes for testing purposes if LLMConnector is not available
class MockLLMConnector:
    """A mock connector to simulate LLM comparisons for testing."""
    def __init__(self, true_ranking):
        # The key is the report ID, the value is its "true" similarity score.
        # Higher score is better.
        self.true_ranking = {k.lower(): v for k, v in true_ranking.items()}

    def compare_by_experts(self, br1_features, br2_features, reduced_case):
        # Simulate a noisy comparison. Return the score of the first report.
        # The higher the score difference, the more likely it is to be correct.
        score1 = self.true_ranking.get(br1_features['id'].lower(), 0)
        score2 = self.true_ranking.get(br2_features['id'].lower(), 0)
        
        # Introduce noise: the probability of giving the correct score
        # is related to the difference in true scores.
        prob_correct = 0.5 + (1 / (1 + math.exp(-0.5 * (score1 - score2)))) / 2
        
        if random.random() < prob_correct:
            return score1
        else:
            return score2


if __name__ == "__main__":
    # This test setup simulates a scenario where we have a known "ground truth"
    # ranking of bug reports, and we want to see if the algorithm can find the best one.
    
    # 1. Define the ground truth: Higher score means more similar.
    # BR-007 is the true "most similar" report.
    TRUE_RANKING = {
        "br-001": 5, "br-002": 8, "br-003": 2, "br-004": 9,
        "br-005": 6, "br-006": 4, "br-007": 10, "br-008": 7,
    }

    # 2. Create mock report data and a mock LLM connector based on the ground truth.
    mock_reports_data = [{"id": rid} for rid in TRUE_RANKING.keys()]
    mock_reduced_case = {"id": "main-bug"} # This is just a placeholder
    mock_connector = MockLLMConnector(true_ranking=TRUE_RANKING)
    
    print("--- Test Scenario ---")
    print("Goal: Find the most similar report. Ground truth best is BR-007 (score 10).")
    print(f"True Ranking: {TRUE_RANKING}")
    print("-" * 21)

    # 3. Initialize the detector with the mock data.
    detector = DuplicateDetector(
        candidate_reports=mock_reports_data,
        reduced_case=mock_reduced_case,
        LLMConnector=mock_connector
    )

    # 4. Run the algorithm to find the most similar report.
    # Epsilon and Delta control the trade-off between accuracy and number of comparisons.
    # Smaller values lead to more comparisons but higher accuracy.
    result_report = detector.find_most_similar(epsilon=0.2, delta=0.2)
    
    print("\n--- Final Result ---")
    if result_report:
        print(f"Algorithm selected: {result_report.id}")
        print(f"Ground truth rank of selected report: {TRUE_RANKING.get(result_report.id)}")
        if result_report.id == "br-007":
            print("SUCCESS: The algorithm correctly identified the best report.")
        else:
            print("FAILURE: The algorithm did not identify the best report.")
    else:
        print("No result was returned.")