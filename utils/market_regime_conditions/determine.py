__all__ = ["determine_regime_by_conditions"]
from typing import Optional, Dict, Tuple
import pandas as pd
from .aggressive_bull import check_aggressive_bull_conditions
from .bull import check_bull_conditions
from .correction import check_correction_conditions
from .risk_management import check_risk_management_conditions
from .bear import check_bear_conditions

def determine_regime_by_conditions(index_data: Dict[str, pd.DataFrame]) -> Tuple[Optional[str], Dict]:
    """필수조건과 부가조건을 기반으로 시장 국면을 판단합니다.
    
    Returns:
        (regime_code, details) - regime_code가 None이면 조건 기반 판단 실패
    """
    all_details = {}
    
    # 각 시장 국면별 조건 검사 (우선순위 순서)
    regime_checks = [
        ('aggressive_bull', check_aggressive_bull_conditions),
        ('bull', check_bull_conditions),
        ('correction', check_correction_conditions),
        ('risk_management', check_risk_management_conditions),
        ('bear', check_bear_conditions)
    ]
    
    qualified_regimes = []
    
    for regime_code, check_function in regime_checks:
        is_qualified, details = check_function(index_data)
        all_details[regime_code] = details
        
        if is_qualified:
            qualified_regimes.append(regime_code)
    
    # 결과 판단
    if len(qualified_regimes) == 1:
        # 정확히 하나의 국면에만 해당
        return qualified_regimes[0], all_details
    elif len(qualified_regimes) == 0:
        # 어느 국면에도 해당하지 않음
        return None, all_details
    else:
        # 여러 국면에 해당 - 우선순위가 높은 것 선택
        for regime_code, _ in regime_checks:
            if regime_code in qualified_regimes:
                return regime_code, all_details
    
    return None, all_details
