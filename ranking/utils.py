from pathlib import Path
from typing import List, Set, Optional
import pandas as pd
import json
import logging

from config import (
    SCREENER_RESULTS_DIR,
    LEADER_STOCK_RESULTS_DIR,
    MOMENTUM_SIGNALS_RESULTS_DIR,
    PORTFOLIO_BUY_DIR,
    MARKET_REGIME_LATEST_PATH,
)
from .criteria_weights import InvestmentStrategy


def _collect_symbols_from_csv(directory: Path, patterns: List[str]) -> Set[str]:
    """Collect ticker symbols from CSV files within ``directory``.

    Rows that explicitly mark a short/sell position are ignored to ensure
    only buy candidates are returned.
    
    For files with timestamps, only the latest file is selected.
    Legacy files and empty files are automatically filtered out.
    """
    collected: Set[str] = set()
    import re
    from datetime import datetime

    for pattern in patterns:
        csv_files = list(directory.glob(pattern))
        if not csv_files:
            continue
        
        # 각 디렉토리별로 최신 파일들을 선택하는 로직 (개선됨)
        files_by_prefix = {}
        
        for csv_path in csv_files:
            # 파일명에서 접두사 추출 (날짜 부분 제거)
            filename = csv_path.stem
            # 날짜 패턴 제거 (_YYYYMMDD 형태)
            prefix = re.sub(r'_\d{8}$', '', filename)
            
            if prefix not in files_by_prefix:
                files_by_prefix[prefix] = []
            files_by_prefix[prefix].append(csv_path)
        
        # 각 접두사별로 최신 파일 선택 (개선된 로직)
        selected_files = []
        for prefix, files in files_by_prefix.items():
            if len(files) == 1:
                selected_files.extend(files)
            else:
                # 날짜가 있는 파일들 중 최신 것 선택 (파일명 기준 우선, 파일 시간 보조)
                dated_files = [f for f in files if re.search(r'_\d{8}$', f.stem)]
                if dated_files:
                    # 파일명의 날짜를 기준으로 최신 파일 선택
                    def get_file_date_priority(file_path):
                        match = re.search(r'_(\d{8})$', file_path.stem)
                        if match:
                            try:
                                date_str = match.group(1)
                                file_date = datetime.strptime(date_str, '%Y%m%d')
                                return (1, file_date)  # 높은 우선순위
                            except ValueError:
                                pass
                        # 파일 시스템 시간 사용
                        file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                        return (0, file_mtime)  # 낮은 우선순위
                    
                    latest_file = max(dated_files, key=get_file_date_priority)
                    selected_files.append(latest_file)
                else:
                    # 날짜가 없는 파일들 중에서는 파일 시간 기준으로 최신 것만 선택
                    latest_file = max(files, key=lambda x: x.stat().st_mtime)
                    selected_files.append(latest_file)
        
        for csv_path in selected_files:
            try:
                from utils.screener_utils import read_csv_flexible
                df = read_csv_flexible(str(csv_path))
                if df is None or df.empty:
                    logging.warning(f"{csv_path}: 읽기 실패 또는 빈 파일 - 건너뜀")
                    continue
                    
                # 데이터가 헤더만 있는 경우 건너뜀
                if len(df) == 0:
                    logging.warning(f"{csv_path}: 헤더만 있는 빈 파일 - 건너뜀")
                    continue
                    
            except Exception as e:
                logging.warning(f"{csv_path}: 읽기 실패 - {e} - 건너뜀")
                continue

            # Filter out short/sell rows if a column indicates position direction
            if "롱여부" in df.columns:
                df = df[df["롱여부"].astype(str).str.lower().isin(["true", "1", "yes"])]
            if "long" in df.columns:
                df = df[df["long"].astype(str).str.lower().isin(["true", "1", "yes"])]
            if "signal" in df.columns:
                df = df[df["signal"].astype(str).str.lower().str.contains("buy")]

            col = None
            if "symbol" in df.columns:
                col = "symbol"
            elif "ticker" in df.columns:
                col = "ticker"
            elif "종목명" in df.columns:
                col = "종목명"

            if col and col in df.columns:
                # Series로 변환하여 .str 속성 사용 가능하도록 함
                series = df[col].dropna().astype(str)
                if hasattr(series, 'str'):
                    collected.update(series.str.upper())
                else:
                    # DataFrame인 경우 첫 번째 컬럼 사용
                    if isinstance(series, pd.DataFrame) and not series.empty:
                        series = series.iloc[:, 0]
                    collected.update(series.astype(str).str.upper())
                    
                logging.info(f"{csv_path}: {len(series)} 개 심볼 수집됨")

    return collected


def load_all_screener_symbols(limit: Optional[int] = None) -> List[str]:
    """Load unique symbols from screener and portfolio result files.

    ``strategy2``와 ``strategy6`` 결과는 제외하고, 그 외의 모든 스크리닝 및
    포트폴리오 매수 결과에 등장하는 종목을 모아 반환한다.
    
    마크미너비니의 기술적 조건 1-8만 적용한 파일(us_with_rs.csv)은 제외한다.
    레거시 파일들과 빈 파일들은 자동으로 필터링된다.
    """

    symbols: Set[str] = set()
    
    # 레거시 파일 목록 (제외할 파일들)
    legacy_files = {
        "image_pattern_results.csv",  # 레거시 파일 (integrated_pattern_results.csv 사용)
    }

    screener_dir = Path(SCREENER_RESULTS_DIR)
    # 모든 CSV 파일을 수집하되, 레거시 파일들은 제외
    all_csv_files = list(screener_dir.glob("**/*.csv"))
    filtered_csv_files = []
    
    for csv_file in all_csv_files:
        # 레거시 파일들 제외
        if csv_file.name in legacy_files:
            logging.info(f"TOPSIS 대상에서 제외: {csv_file} (레거시 파일 또는 기술적 조건만 적용)")
            continue
        filtered_csv_files.append(str(csv_file.relative_to(screener_dir)))
    
    symbols.update(_collect_symbols_from_csv(screener_dir, filtered_csv_files))

    leader_dir = Path(LEADER_STOCK_RESULTS_DIR)
    symbols.update(_collect_symbols_from_csv(leader_dir, ["*.csv"]))

    momentum_dir = Path(MOMENTUM_SIGNALS_RESULTS_DIR)
    symbols.update(_collect_symbols_from_csv(momentum_dir, ["*.csv"]))

    portfolio_dir = Path(PORTFOLIO_BUY_DIR)
    # 동적으로 strategy 파일들을 찾되, strategy2/6은 제외 (매도 전략)
    if portfolio_dir.exists():
        strategy_files = []
        for file_path in portfolio_dir.glob("strategy*_results.csv"):
            # strategy2와 strategy6은 매도 전략이므로 제외
            if not any(excluded in file_path.name for excluded in ["strategy2_", "strategy6_"]):
                strategy_files.append(file_path.name)
        symbols.update(_collect_symbols_from_csv(portfolio_dir, strategy_files))

    symbol_list = sorted(symbols)
    if limit is not None:
        return symbol_list[:limit]
    return symbol_list


def get_market_regime_strategy(
    default: InvestmentStrategy = InvestmentStrategy.BALANCED,
) -> InvestmentStrategy:
    "Return an investment strategy mapped from the latest market regime." 
    path = Path(MARKET_REGIME_LATEST_PATH)
    if not path.exists():
        return default

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        regime = str(data.get("regime", "")).lower()
    except Exception as e:
        logging.warning(f"Failed to read market regime: {e}")
        return default

    if regime in {"bull", "aggressive_bull"}:
        return InvestmentStrategy.AGGRESSIVE
    if regime in {"bear", "risk_management"}:
        return InvestmentStrategy.RISK_AVERSE
    return default
