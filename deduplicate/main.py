import json
import numpy as np
import pandas as pd
import re
import logging
import sys
from collections import Counter
import time
from scipy.sparse import issparse

# 本地库
import llmConnect
import dataLoader
import resultStat
from code import DuplicateDetector

def get_top_ids(new_vector, case_database, X_one_hot_weighted, y, reports, lim=32):
    """
    Finds top candidate IDs based on vector similarity, pre-filtered by database.

    Args:
        new_vector: The vector of the new case.
        case_database (str): The database name of the new case (e.g., 'MySQL', 'PostgreSQL').
        X_one_hot_weighted: The matrix of all vectors in the knowledge base.
        y (pd.Series): A series mapping rows in X to their report IDs.
        reports (pd.DataFrame): The DataFrame containing all report details, including the database name.
        lim (int): The maximum number of candidate IDs to return.

    Returns:
        list: A list of candidate report IDs from the same database, sorted by similarity.
    """
    # --- Step 1: Filter the knowledge base to include only reports from the same database ---
    # Create a boolean mask. Assumes the first column of `reports` is the database name.
    # .str.lower() makes the comparison case-insensitive.
    db_mask = (reports.iloc[:, 0].str.lower() == case_database.lower())
    
    # Apply the mask to the main vector matrix and the ID series.
    # This assumes that the rows in `reports`, `X`, and `y` are aligned.
    X_filtered = X_one_hot_weighted[db_mask]
    y_filtered = y[db_mask]

    # If no reports from this database exist in our knowledge base, return an empty list.
    if X_filtered.shape[0] == 0:
        logging.warning(f"No reports found for database '{case_database}' in the knowledge base.")
        return []

    # --- Step 2: Perform similarity search on the filtered set ---
    # Handle sparse/dense matrix formats
    if issparse(X_filtered):
        X_filtered_dense = X_filtered.toarray()
    else:
        X_filtered_dense = X_filtered
    
    if issparse(new_vector):
        new_vector_dense = new_vector.toarray().flatten()
    else:
        new_vector_dense = new_vector.flatten()
    
    # Calculate dot products (similarity) only on the filtered data
    dot_products = np.dot(X_filtered_dense, new_vector_dense)
    
    # Ensure we don't request more items than available in the filtered set
    num_to_retrieve = min(lim, len(y_filtered))
    
    # Get the indices of the top N most similar vectors from the *filtered* set
    top_indices_in_filtered = np.argsort(dot_products)[-num_to_retrieve:][::-1]
    
    # Use these indices to look up the corresponding IDs from the *filtered* ID series
    top_ids = y_filtered.iloc[top_indices_in_filtered].values    
    return list(top_ids)

if __name__ == '__main__':
    connector = llmConnect.LLMConnector()
    dataLoader = dataLoader.DataLoader()
    for x in [16]:
        print(f'filter number: {x}')
        start_time = time.time()
        
        # Load all necessary data
        new_data_one_hot = dataLoader.new_data_one_hot
        case_data = dataLoader.case_data
        X_one_hot_weighted = dataLoader.X_one_hot_weighted
        y = dataLoader.y
        reports = dataLoader.reports # DataFrame of existing reports
        cases = dataLoader.cases     # Dict of new cases for testing

        # --- Stage 1: Generate candidate lists for all new cases ---
        results = []
        for i, new_vector in enumerate(new_data_one_hot):
            new_case_id = case_data['id'].iloc[i]
            new_case_duplicate_id = case_data['Duplicate ID'].iloc[i]
            
            # Get the database for the current new case
            case_info = cases[new_case_id]
            case_db = case_info['database']

            # Call the refactored function with the database info
            candidate_ids = get_top_ids(
                new_vector, 
                case_db, 
                X_one_hot_weighted, 
                y, 
                reports, 
                lim=x
            )

            results.append((new_case_id, new_case_duplicate_id, candidate_ids))

        # --- Stage 2: Process the results with the LLM ---
        result_rank = []
        for result in results:
            new_case_id, new_case_duplicate_id, top_matches = result

            # Since get_top_ids can return an empty list if no matching DB is found
            if not top_matches:
                print(f"Skipping case {new_case_id} as no candidates from the same DB were found.")
                result_rank.append(-1) # Or some other indicator for 'not found'
                continue

            # Get the full report data for the candidates
            candidate_reports = reports[reports['id'].isin(top_matches)].values.tolist()
            
            # This check is still useful for debugging data integrity
            _candidate_ids = set(report[-1] for report in candidate_reports)
            _missing_ids = list(set(top_matches) - _candidate_ids)
            if _missing_ids:
                logging.warning(f"Could not find full report data for IDs: {_missing_ids}")

            # The case info is needed for the DuplicateDetector
            case_info = cases[new_case_id]

            # !! The database filtering block that was here is NO LONGER NEEDED !!
            # candidate_reports is already pre-filtered by database.
            
            detector = DuplicateDetector(candidate_reports, case_info, connector)
            rank = detector.deduplicate(new_case_duplicate_id)
            
            result_rank.append(rank)
            
        print(f"token cost: {connector.get_token_usage()['total_tokens']}")
        rs = resultStat.ResultStat()
        rs.calculate_all_recall(results)
        rs.print_result_rank(result_rank)
        print(f'time cost: {time.time() - start_time}', flush=True)