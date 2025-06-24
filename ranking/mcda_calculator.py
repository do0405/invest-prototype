"""Multi-Criteria Decision Analysis (MCDA) calculator for stock ranking.

Implements various MCDA methods including TOPSIS, VIKOR, and weighted sum models
for comprehensive stock evaluation and ranking.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Union
from enum import Enum
import warnings

class CriteriaType(Enum):
    """Criteria type enumeration."""
    BENEFIT = "benefit"  # Higher values are better (e.g., ROE, profit margin)
    COST = "cost"       # Lower values are better (e.g., P/E ratio, debt ratio)

class NormalizationMethod(Enum):
    """Normalization method enumeration."""
    MIN_MAX = "min_max"
    VECTOR = "vector"
    SUM = "sum"
    MAX = "max"

class MCDAMethod(Enum):
    """MCDA method enumeration."""
    TOPSIS = "topsis"
    VIKOR = "vikor"
    WEIGHTED_SUM = "weighted_sum"
    COPRAS = "copras"

class MCDACalculator:
    """Multi-Criteria Decision Analysis calculator.
    
    This class implements various MCDA methods for stock ranking based on
    multiple criteria with different importance weights.
    """
    
    def __init__(self):
        self.decision_matrix = None
        self.criteria_weights = None
        self.criteria_types = None
        self.normalized_matrix = None
        
    def set_decision_matrix(self, 
                           matrix: pd.DataFrame,
                           criteria_weights: Dict[str, float],
                           criteria_types: Dict[str, CriteriaType]) -> None:
        """Set the decision matrix and criteria information.
        
        Args:
            matrix: Decision matrix with alternatives as rows and criteria as columns
            criteria_weights: Dictionary mapping criteria names to weights (should sum to 1)
            criteria_types: Dictionary mapping criteria names to CriteriaType
        """
        self.decision_matrix = matrix.copy()
        self.criteria_weights = criteria_weights.copy()
        self.criteria_types = criteria_types.copy()
        
        # Validate inputs
        self._validate_inputs()
        
    def _validate_inputs(self) -> None:
        """Validate input data consistency."""
        if self.decision_matrix is None:
            raise ValueError("Decision matrix not set")
            
        # Check if all criteria have weights and types
        matrix_criteria = set(self.decision_matrix.columns)
        weight_criteria = set(self.criteria_weights.keys())
        type_criteria = set(self.criteria_types.keys())
        
        if matrix_criteria != weight_criteria:
            raise ValueError(f"Criteria mismatch between matrix and weights: {matrix_criteria - weight_criteria}")
            
        if matrix_criteria != type_criteria:
            raise ValueError(f"Criteria mismatch between matrix and types: {matrix_criteria - type_criteria}")
            
        # Check if weights sum to approximately 1
        weight_sum = sum(self.criteria_weights.values())
        if abs(weight_sum - 1.0) > 1e-6:
            warnings.warn(f"Criteria weights sum to {weight_sum:.6f}, not 1.0. Normalizing weights.")
            # Normalize weights
            for criterion in self.criteria_weights:
                self.criteria_weights[criterion] /= weight_sum
                
    def normalize_matrix(self, method: NormalizationMethod = NormalizationMethod.VECTOR) -> pd.DataFrame:
        """Normalize the decision matrix.
        
        Args:
            method: Normalization method to use
            
        Returns:
            Normalized decision matrix
        """
        if self.decision_matrix is None:
            raise ValueError("Decision matrix not set")
            
        matrix = self.decision_matrix.copy()
        normalized = matrix.copy()
        
        for column in matrix.columns:
            col_data = matrix[column].values
            
            if method == NormalizationMethod.MIN_MAX:
                min_val, max_val = col_data.min(), col_data.max()
                if max_val - min_val != 0:
                    if self.criteria_types[column] == CriteriaType.BENEFIT:
                        normalized[column] = (col_data - min_val) / (max_val - min_val)
                    else:  # COST
                        normalized[column] = (max_val - col_data) / (max_val - min_val)
                else:
                    normalized[column] = 0.5  # All values are the same
                    
            elif method == NormalizationMethod.VECTOR:
                norm = np.sqrt(np.sum(col_data ** 2))
                if norm != 0:
                    normalized[column] = col_data / norm
                else:
                    normalized[column] = 0
                    
            elif method == NormalizationMethod.SUM:
                sum_val = np.sum(col_data)
                if sum_val != 0:
                    normalized[column] = col_data / sum_val
                else:
                    normalized[column] = 1 / len(col_data)
                    
            elif method == NormalizationMethod.MAX:
                max_val = col_data.max()
                if max_val != 0:
                    normalized[column] = col_data / max_val
                else:
                    normalized[column] = 1
                    
        self.normalized_matrix = normalized
        return normalized
        
    def calculate_topsis(self, normalization: NormalizationMethod = NormalizationMethod.VECTOR) -> pd.DataFrame:
        """Calculate TOPSIS (Technique for Order Preference by Similarity to Ideal Solution).
        
        Args:
            normalization: Normalization method to use
            
        Returns:
            DataFrame with TOPSIS scores and rankings
        """
        # Normalize matrix
        normalized = self.normalize_matrix(normalization)
        
        # Apply weights
        weighted = normalized.copy()
        for column in weighted.columns:
            weighted[column] *= self.criteria_weights[column]
            
        # Determine ideal and negative-ideal solutions
        ideal_solution = {}
        negative_ideal_solution = {}
        
        for column in weighted.columns:
            if self.criteria_types[column] == CriteriaType.BENEFIT:
                ideal_solution[column] = weighted[column].max()
                negative_ideal_solution[column] = weighted[column].min()
            else:  # COST
                ideal_solution[column] = weighted[column].min()
                negative_ideal_solution[column] = weighted[column].max()
                
        # Calculate distances
        distances_to_ideal = []
        distances_to_negative_ideal = []
        
        for idx in weighted.index:
            # Distance to ideal solution
            d_ideal = np.sqrt(sum([
                (weighted.loc[idx, col] - ideal_solution[col]) ** 2
                for col in weighted.columns
            ]))
            distances_to_ideal.append(d_ideal)
            
            # Distance to negative-ideal solution
            d_negative = np.sqrt(sum([
                (weighted.loc[idx, col] - negative_ideal_solution[col]) ** 2
                for col in weighted.columns
            ]))
            distances_to_negative_ideal.append(d_negative)
            
        # Calculate relative closeness
        closeness = []
        for i in range(len(distances_to_ideal)):
            if distances_to_ideal[i] + distances_to_negative_ideal[i] != 0:
                c = distances_to_negative_ideal[i] / (distances_to_ideal[i] + distances_to_negative_ideal[i])
            else:
                c = 0.5
            closeness.append(c)
            
        # Create results DataFrame
        results = pd.DataFrame({
            'alternative': weighted.index,
            'topsis_score': closeness,
            'distance_to_ideal': distances_to_ideal,
            'distance_to_negative_ideal': distances_to_negative_ideal
        })
        
        # Add ranking (higher score = better rank)
        results['topsis_rank'] = results['topsis_score'].rank(ascending=False, method='min')
        
        return results.set_index('alternative')
        
    def calculate_vikor(self, 
                       normalization: NormalizationMethod = NormalizationMethod.MIN_MAX,
                       v: float = 0.5) -> pd.DataFrame:
        """Calculate VIKOR (VIseKriterijumska Optimizacija I Kompromisno Resenje).
        
        Args:
            normalization: Normalization method to use
            v: Weight for the strategy of maximum group utility (0 ≤ v ≤ 1)
            
        Returns:
            DataFrame with VIKOR scores and rankings
        """
        # Normalize matrix
        normalized = self.normalize_matrix(normalization)
        
        # For VIKOR, we need to work with the original scale but consider ideal solutions
        matrix = self.decision_matrix.copy()
        
        # Determine best and worst values for each criterion
        best_values = {}
        worst_values = {}
        
        for column in matrix.columns:
            if self.criteria_types[column] == CriteriaType.BENEFIT:
                best_values[column] = matrix[column].max()
                worst_values[column] = matrix[column].min()
            else:  # COST
                best_values[column] = matrix[column].min()
                worst_values[column] = matrix[column].max()
                
        # Calculate S and R values
        S_values = []
        R_values = []
        
        for idx in matrix.index:
            S = 0  # Sum of weighted normalized distances
            R = 0  # Maximum weighted normalized distance
            
            for column in matrix.columns:
                weight = self.criteria_weights[column]
                value = matrix.loc[idx, column]
                best = best_values[column]
                worst = worst_values[column]
                
                if worst != best:
                    normalized_distance = (best - value) / (best - worst)
                else:
                    normalized_distance = 0
                    
                weighted_distance = weight * normalized_distance
                S += weighted_distance
                R = max(R, weighted_distance)
                
            S_values.append(S)
            R_values.append(R)
            
        # Calculate Q values
        S_best = min(S_values)
        S_worst = max(S_values)
        R_best = min(R_values)
        R_worst = max(R_values)
        
        Q_values = []
        for i in range(len(S_values)):
            if (S_worst - S_best) != 0 and (R_worst - R_best) != 0:
                Q = v * (S_values[i] - S_best) / (S_worst - S_best) + \
                    (1 - v) * (R_values[i] - R_best) / (R_worst - R_best)
            else:
                Q = 0
            Q_values.append(Q)
            
        # Create results DataFrame
        results = pd.DataFrame({
            'alternative': matrix.index,
            'S': S_values,
            'R': R_values,
            'Q': Q_values
        })
        
        # Add rankings (lower values = better ranks for VIKOR)
        results['vikor_rank'] = results['Q'].rank(ascending=True, method='min')
        
        return results.set_index('alternative')
        
    def calculate_weighted_sum(self, normalization: NormalizationMethod = NormalizationMethod.MIN_MAX) -> pd.DataFrame:
        """Calculate Weighted Sum Model (WSM).
        
        Args:
            normalization: Normalization method to use
            
        Returns:
            DataFrame with weighted sum scores and rankings
        """
        # Normalize matrix
        normalized = self.normalize_matrix(normalization)
        
        # Calculate weighted sum for each alternative
        scores = []
        for idx in normalized.index:
            score = sum([
                normalized.loc[idx, col] * self.criteria_weights[col]
                for col in normalized.columns
            ])
            scores.append(score)
            
        # Create results DataFrame
        results = pd.DataFrame({
            'alternative': normalized.index,
            'weighted_sum_score': scores
        })
        
        # Add ranking (higher score = better rank)
        results['weighted_sum_rank'] = results['weighted_sum_score'].rank(ascending=False, method='min')
        
        return results.set_index('alternative')
        
    def calculate_copras(self, normalization: NormalizationMethod = NormalizationMethod.SUM) -> pd.DataFrame:
        """Calculate COPRAS (COmplex PRoportional ASsessment).
        
        Args:
            normalization: Normalization method to use
            
        Returns:
            DataFrame with COPRAS scores and rankings
        """
        # Normalize matrix using sum normalization for COPRAS
        normalized = self.normalize_matrix(normalization)
        
        # Apply weights
        weighted = normalized.copy()
        for column in weighted.columns:
            weighted[column] *= self.criteria_weights[column]
            
        # Calculate sums for benefit and cost criteria
        scores = []
        for idx in weighted.index:
            S_plus = sum([  # Sum of beneficial criteria
                weighted.loc[idx, col] for col in weighted.columns
                if self.criteria_types[col] == CriteriaType.BENEFIT
            ])
            
            S_minus = sum([  # Sum of cost criteria
                weighted.loc[idx, col] for col in weighted.columns
                if self.criteria_types[col] == CriteriaType.COST
            ])
            
            scores.append((S_plus, S_minus))
            
        # Calculate relative significance
        S_minus_min = min([s[1] for s in scores]) if any(s[1] > 0 for s in scores) else 1
        
        Q_values = []
        for S_plus, S_minus in scores:
            if S_minus > 0:
                Q = S_plus + (S_minus_min * sum([s[1] for s in scores])) / (S_minus * len(scores))
            else:
                Q = S_plus
            Q_values.append(Q)
            
        # Calculate utility degree
        Q_max = max(Q_values)
        utility_degrees = [Q / Q_max * 100 for Q in Q_values]
        
        # Create results DataFrame
        results = pd.DataFrame({
            'alternative': weighted.index,
            'S_plus': [s[0] for s in scores],
            'S_minus': [s[1] for s in scores],
            'Q': Q_values,
            'copras_score': utility_degrees
        })
        
        # Add ranking (higher score = better rank)
        results['copras_rank'] = results['copras_score'].rank(ascending=False, method='min')
        
        return results.set_index('alternative')
        
    def calculate_all_methods(self, 
                             methods: List[MCDAMethod] = None,
                             normalization: NormalizationMethod = NormalizationMethod.VECTOR) -> pd.DataFrame:
        """Calculate rankings using multiple MCDA methods.
        
        Args:
            methods: List of MCDA methods to use (default: all methods)
            normalization: Normalization method to use
            
        Returns:
            DataFrame with results from all methods
        """
        if methods is None:
            methods = [MCDAMethod.TOPSIS, MCDAMethod.VIKOR, MCDAMethod.WEIGHTED_SUM, MCDAMethod.COPRAS]
            
        results = pd.DataFrame(index=self.decision_matrix.index)
        
        for method in methods:
            if method == MCDAMethod.TOPSIS:
                topsis_results = self.calculate_topsis(normalization)
                results['topsis_score'] = topsis_results['topsis_score']
                results['topsis_rank'] = topsis_results['topsis_rank']
                
            elif method == MCDAMethod.VIKOR:
                vikor_results = self.calculate_vikor(normalization)
                results['vikor_score'] = vikor_results['Q']
                results['vikor_rank'] = vikor_results['vikor_rank']
                
            elif method == MCDAMethod.WEIGHTED_SUM:
                wsm_results = self.calculate_weighted_sum(normalization)
                results['weighted_sum_score'] = wsm_results['weighted_sum_score']
                results['weighted_sum_rank'] = wsm_results['weighted_sum_rank']
                
            elif method == MCDAMethod.COPRAS:
                copras_results = self.calculate_copras(NormalizationMethod.SUM)
                results['copras_score'] = copras_results['copras_score']
                results['copras_rank'] = copras_results['copras_rank']
                
        return results
        
    def calculate_consensus_ranking(self, 
                                  methods: List[MCDAMethod] = None,
                                  method_weights: Dict[MCDAMethod, float] = None) -> pd.DataFrame:
        """Calculate consensus ranking from multiple MCDA methods.
        
        Args:
            methods: List of MCDA methods to use
            method_weights: Weights for each method (default: equal weights)
            
        Returns:
            DataFrame with consensus ranking
        """
        if methods is None:
            methods = [MCDAMethod.TOPSIS, MCDAMethod.VIKOR, MCDAMethod.WEIGHTED_SUM, MCDAMethod.COPRAS]
            
        if method_weights is None:
            method_weights = {method: 1.0 / len(methods) for method in methods}
            
        # Get results from all methods
        all_results = self.calculate_all_methods(methods)
        
        # Calculate weighted average of ranks
        consensus_scores = []
        for idx in all_results.index:
            weighted_rank_sum = 0
            total_weight = 0
            
            for method in methods:
                rank_col = f"{method.value}_rank"
                if rank_col in all_results.columns:
                    # Convert rank to score (lower rank = higher score)
                    rank = all_results.loc[idx, rank_col]
                    score = len(all_results) - rank + 1  # Invert rank to score
                    weighted_rank_sum += score * method_weights[method]
                    total_weight += method_weights[method]
                    
            if total_weight > 0:
                consensus_score = weighted_rank_sum / total_weight
            else:
                consensus_score = 0
                
            consensus_scores.append(consensus_score)
            
        # Create consensus results
        consensus_results = pd.DataFrame({
            'alternative': all_results.index,
            'consensus_score': consensus_scores
        })
        
        # Add consensus ranking
        consensus_results['consensus_rank'] = consensus_results['consensus_score'].rank(ascending=False, method='min')
        
        # Merge with individual method results
        final_results = all_results.copy()
        final_results['consensus_score'] = consensus_results.set_index('alternative')['consensus_score']
        final_results['consensus_rank'] = consensus_results.set_index('alternative')['consensus_rank']
        
        return final_results