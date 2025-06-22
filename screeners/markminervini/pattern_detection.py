

from __future__ import annotations

import os
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime

import pandas as pd
import numpy as np
from scipy.signal import find_peaks, argrelextrema
from scipy.stats import pearsonr

# 로깅 설정
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# -----------------------------------------------------
# Academic Paper-based Detection Algorithms
# -----------------------------------------------------

def kernel_smoothing(prices: np.ndarray, bandwidth: float = None) -> np.ndarray:
    """비모수 커널 회귀를 이용한 가격 곡선 스무딩 (Lo, Mamaysky & Wang 2000)
    
    Args:
        prices: 가격 시계열 데이터
        bandwidth: 커널 대역폭 (None이면 자동 계산)
        
    Returns:
        np.ndarray: 스무딩된 가격 곡선
    """
    n = len(prices)
    if n < 10:
        return prices
    
    # 최적 대역폭 계산 (CV 최적값의 30% 수준으로 조정)
    if bandwidth is None:
        # Silverman's rule of thumb 기반 대역폭
        std_prices = np.std(prices)
        bandwidth = 1.06 * std_prices * (n ** (-1/5)) * 0.3
    
    smoothed = np.zeros_like(prices)
    x_points = np.arange(n)
    
    for i in range(n):
        # 가우시안 커널 가중치 계산
        weights = np.exp(-0.5 * ((x_points - i) / bandwidth) ** 2)
        weights /= np.sum(weights)
        
        # 가중 평균으로 스무딩 값 계산
        smoothed[i] = np.sum(weights * prices)
    
    return smoothed


def extract_peaks_troughs(smoothed_prices: np.ndarray, min_distance: int = 5) -> Tuple[np.ndarray, np.ndarray]:
    """스무딩된 곡선에서 피크와 골 추출
    
    Args:
        smoothed_prices: 스무딩된 가격 데이터
        min_distance: 피크 간 최소 거리
        
    Returns:
        Tuple[np.ndarray, np.ndarray]: (피크 인덱스, 골 인덱스)
    """
    # 피크 찾기
    peaks, _ = find_peaks(smoothed_prices, distance=min_distance)
    
    # 골 찾기 (음수로 변환 후 피크 찾기)
    troughs, _ = find_peaks(-smoothed_prices, distance=min_distance)
    
    return peaks, troughs


def calculate_amplitude_contraction(peaks: np.ndarray, troughs: np.ndarray, prices: np.ndarray) -> List[float]:
    """연속 피크 간 진폭 수축 계산 (Suh, Li & Gao 2008)
    
    Args:
        peaks: 피크 인덱스 배열
        troughs: 골 인덱스 배열  
        prices: 가격 데이터
        
    Returns:
        List[float]: 각 구간의 진폭 비율
    """
    if len(peaks) < 2:
        return []
    
    amplitudes = []
    
    for i in range(len(peaks) - 1):
        peak1_idx = peaks[i]
        peak2_idx = peaks[i + 1]
        
        # 두 피크 사이의 최저점 찾기
        between_troughs = troughs[(troughs > peak1_idx) & (troughs < peak2_idx)]
        if len(between_troughs) > 0:
            trough_idx = between_troughs[np.argmin(prices[between_troughs])]
            
            # 진폭 계산 (피크에서 골까지의 최대 하락폭)
            amplitude = max(
                prices[peak1_idx] - prices[trough_idx],
                prices[peak2_idx] - prices[trough_idx]
            )
            amplitudes.append(amplitude)
    
    return amplitudes


def quadratic_fit_cup(cup_indices: np.ndarray, prices: np.ndarray) -> Tuple[float, float]:
    """2차 다항식 근사를 이용한 U자형 컵 검증 (Suh, Li & Gao 2008)
    
    Args:
        cup_indices: 컵 구간의 인덱스
        prices: 해당 구간의 가격 데이터
        
    Returns:
        Tuple[float, float]: (R-squared, 곡률)
    """
    if len(cup_indices) < 3:
        return 0.0, 0.0
    
    try:
        # 2차 다항식 피팅: f(t) = at^2 + bt + c
        coeffs = np.polyfit(cup_indices, prices, 2)
        fitted_prices = np.polyval(coeffs, cup_indices)
        
        # R-squared 계산
        ss_res = np.sum((prices - fitted_prices) ** 2)
        ss_tot = np.sum((prices - np.mean(prices)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        
        # 곡률 계산 (2차 계수의 절댓값)
        curvature = abs(coeffs[0])
        
        return r_squared, curvature
    except:
        return 0.0, 0.0


def bezier_curve_correlation(control_points: np.ndarray, actual_prices: np.ndarray) -> float:
    """베지어 곡선과 실제 가격의 상관계수 계산 (Suh, Li & Gao 2008)
    
    Args:
        control_points: 베지어 곡선 제어점
        actual_prices: 실제 가격 데이터
        
    Returns:
        float: 피어슨 상관계수
    """
    if len(control_points) < 3 or len(actual_prices) < 3:
        return 0.0
    
    try:
        # 간단한 베지어 곡선 근사 (3차 다항식 사용)
        t = np.linspace(0, 1, len(actual_prices))
        
        # 제어점을 이용한 베지어 곡선 생성
        if len(control_points) >= 4:
            # 3차 베지어 곡선
            bezier_curve = (
                (1-t)**3 * control_points[0] +
                3*(1-t)**2*t * control_points[1] +
                3*(1-t)*t**2 * control_points[2] +
                t**3 * control_points[3]
            )
        else:
            # 2차 베지어 곡선
            bezier_curve = (
                (1-t)**2 * control_points[0] +
                2*(1-t)*t * control_points[1] +
                t**2 * control_points[2]
            )
        
        # 피어슨 상관계수 계산
        correlation, _ = pearsonr(bezier_curve, actual_prices)
        return correlation if not np.isnan(correlation) else 0.0
    except:
        return 0.0


def detect_vcp(df: pd.DataFrame) -> bool:
    """학술 논문 기반 VCP 패턴 감지 (Lo, Mamaysky & Wang 2000; Suh, Li & Gao 2008)
    
    Args:
        df: 주가 데이터 (최소 60일 이상의 데이터 필요)
        
    Returns:
        bool: VCP 패턴 감지 여부
    """
    if df is None or len(df) < 60:
        return False

    # 최근 90일 데이터 사용
    recent = df.tail(90).copy()
    prices = recent["close"].values
    volumes = recent["volume"].values
    
    # 1. 커널 회귀를 이용한 가격 곡선 스무딩
    smoothed_prices = kernel_smoothing(prices)
    
    # 2. 스무딩된 곡선에서 피크와 골 추출
    peaks, troughs = extract_peaks_troughs(smoothed_prices)
    
    if len(peaks) < 3:  # 최소 3개의 피크 필요
        return False
    
    # 3. 연속적 변동성 수축 검출
    amplitudes = calculate_amplitude_contraction(peaks, troughs, smoothed_prices)
    
    if len(amplitudes) < 2:  # 최소 2회 수축 필요
        return False
    
    # 4. 진폭 감소 패턴 확인
    contraction_count = 0
    for i in range(1, len(amplitudes)):
        if amplitudes[i] < amplitudes[i-1] * 0.85:  # 15% 이상 감소
            contraction_count += 1
    
    if contraction_count < 2:  # 최소 2회 연속 수축
        return False
    
    # 5. 거래량 패턴 확인 (수축 시 거래량 감소)
    volume_ma = pd.Series(volumes).rolling(10).mean().values
    recent_volume_trend = volume_ma[-10:]
    
    if len(recent_volume_trend) > 5:
        volume_decrease = recent_volume_trend[-1] < recent_volume_trend[0] * 0.8
        if not volume_decrease:
            return False
    
    # 6. 최종 브레이크아웃 확인
    last_peak_price = smoothed_prices[peaks[-1]]
    current_price = prices[-1]
    
    # 현재 가격이 마지막 피크 근처에 있어야 함
    if current_price < last_peak_price * 0.95:
        return False
    
    return True


def detect_cup_and_handle(df: pd.DataFrame, window: int = 180) -> bool:
    """학술 논문 기반 Cup-with-Handle 패턴 감지 (Suh, Li & Gao 2008)
    
    Args:
        df: 주가 데이터 (최소 window일 이상의 데이터 필요)
        window: 분석할 기간 (기본값: 180일)
        
    Returns:
        bool: Cup-with-Handle 패턴 감지 여부
    """
    if df is None or len(df) < window:
        return False

    data = df.tail(window).copy()
    prices = data["close"].values
    volumes = data["volume"].values
    
    # 1. 커널 회귀를 이용한 가격 곡선 스무딩
    smoothed_prices = kernel_smoothing(prices)
    
    # 2. 피크와 골 추출
    peaks, troughs = extract_peaks_troughs(smoothed_prices)
    
    if len(peaks) < 2 or len(troughs) == 0:
        return False

    # 3. 컵 구조 식별 (좌측 고점 - 바닥 - 우측 고점)
    left_peak = peaks[0]
    right_candidates = peaks[peaks > left_peak]
    if len(right_candidates) == 0:
        return False
    
    right_peak = right_candidates[-1]
    bottom_candidates = troughs[(troughs > left_peak) & (troughs < right_peak)]
    if len(bottom_candidates) == 0:
        return False
    
    bottom = bottom_candidates[np.argmin(smoothed_prices[bottom_candidates])]

    # 4. 기본 구조 검증
    if right_peak - left_peak < 30:  # 최소 30일 컵 형성 기간
        return False
    
    if bottom - left_peak < 8 or right_peak - bottom < 8:  # 좌우 균형
        return False

    # 5. 2차 다항식 근사를 이용한 U자형 컵 검증
    cup_indices = np.arange(left_peak, right_peak + 1)
    cup_prices = smoothed_prices[left_peak:right_peak + 1]
    
    r_squared, curvature = quadratic_fit_cup(cup_indices, cup_prices)
    
    if r_squared < 0.7:  # R-squared 임계값
        return False
    
    if curvature < 0.0001:  # 충분한 곡률 필요
        return False

    # 6. 베지어 곡선 상관계수 검증
    # 7개 제어점 선정: 좌측 고점, 중간점들, 바닥, 우측 고점
    control_points = np.array([
        smoothed_prices[left_peak],
        smoothed_prices[left_peak + (bottom - left_peak) // 2],
        smoothed_prices[bottom],
        smoothed_prices[bottom + (right_peak - bottom) // 2],
        smoothed_prices[right_peak]
    ])
    
    correlation = bezier_curve_correlation(control_points, cup_prices)
    
    if correlation < 0.85:  # 논문에서 제시한 임계값
        return False

    # 7. 좌우 고점 대칭성 검증
    left_high = smoothed_prices[left_peak]
    right_high = smoothed_prices[right_peak]
    height_diff = abs(left_high - right_high) / min(left_high, right_high) * 100
    
    if height_diff > 5:  # 5% 이내 차이
        return False

    # 8. 컵 깊이 검증
    bottom_low = smoothed_prices[bottom]
    depth = (min(left_high, right_high) - bottom_low) / min(left_high, right_high) * 100
    
    if not (12 <= depth <= 50):  # 적절한 깊이
        return False

    # 9. 핸들 검증
    handle_start = right_peak
    handle_prices = smoothed_prices[handle_start:]
    
    if len(handle_prices) < 5:  # 최소 핸들 길이
        return False
    
    handle_low = np.min(handle_prices)
    handle_depth = (right_high - handle_low) / right_high * 100
    
    # 핸들 깊이가 컵 깊이의 33% 이내 (논문 기준)
    if handle_depth > depth * 0.33:
        return False
    
    if handle_depth < 2 or handle_depth > 25:  # 적절한 핸들 깊이
        return False

    # 10. 거래량 패턴 검증
    avg_volume = pd.Series(volumes).rolling(20).mean().values
    
    # 컵 바닥에서 거래량 감소
    bottom_vol = volumes[max(0, bottom - 2): bottom + 3].mean()
    if bottom_vol >= avg_volume[bottom] * 0.7:
        return False
    
    # 핸들 구간 거래량 확인
    handle_vol = volumes[handle_start:].mean() if len(volumes[handle_start:]) > 0 else volumes[handle_start]
    if handle_vol >= bottom_vol * 1.2:
        return False
    
    # 최근 브레이크아웃 거래량
    if len(volumes) > 0 and len(avg_volume) > 0:
        if volumes[-1] < avg_volume[-1] * 1.2:
            return False

    # 11. 컵 형성 중 거래량 감소 트렌드
    cup_volumes = volumes[left_peak:right_peak]
    if len(cup_volumes) > 10:
        early_vol = cup_volumes[:len(cup_volumes)//3].mean()
        late_vol = cup_volumes[-len(cup_volumes)//3:].mean()
        if late_vol >= early_vol * 1.1:  # 후반부 거래량이 너무 증가하면 안됨
            return False

    return True


# -----------------------------------------------------
# Batch analysis
# -----------------------------------------------------

from config import MARKMINERVINI_RESULTS_DIR


def analyze_tickers_from_results(results_dir: str, data_dir: str, output_dir: str = MARKMINERVINI_RESULTS_DIR) -> pd.DataFrame:
    """CSV 파일에서 티커 목록을 읽고 패턴을 감지하여 결과를 반환합니다.
    
    Args:
        results_dir: 재무 결과 파일이 있는 디렉토리 경로
        data_dir: 주가 데이터 CSV 파일이 있는 디렉토리 경로
        output_dir: 결과를 저장할 디렉토리 경로
        
    Returns:
        pd.DataFrame: 패턴 감지 결과
        
    Raises:
        FileNotFoundError: 결과 파일이 존재하지 않을 경우
    """
    os.makedirs(output_dir, exist_ok=True)
    results_file = os.path.join(results_dir, "advanced_financial_results.csv")
    if not os.path.exists(results_file):
        raise FileNotFoundError(f"결과 파일을 찾을 수 없습니다: {results_file}")

    logger.info(f"재무 결과 파일 로드 중: {results_file}")
    results_df = pd.read_csv(results_file)
    logger.info(f"총 {len(results_df)}개 종목에 대한 패턴 분석 시작")
    
    analysis = []

    for _, row in results_df.iterrows():
        symbol = row['symbol']
        fin_met_count = row.get('fin_met_count', 0)
        # fin_met_count 조건 제거 - 모든 종목에 대해 패턴 분석 수행
        file_path = os.path.join(data_dir, f"{symbol}.csv")
        if not os.path.exists(file_path):
            continue
        df = pd.read_csv(file_path)
        date_col = next((c for c in df.columns if c.lower() in ['date', '날짜', '일자']), None)
        if not date_col:
            continue
        df[date_col] = pd.to_datetime(df[date_col], utc=True)
        df.set_index(date_col, inplace=True)
        col_map = {'high': ['high', 'High', '고가'], 'low': ['low', 'Low', '저가'], 'close': ['close', 'Close', '종가'], 'volume': ['volume', 'Volume', '거래량']}
        found = {}
        for k, names in col_map.items():
            for c in df.columns:
                if c.lower() in [n.lower() for n in names]:
                    found[k] = c
                    break
        if len(found) < 4:
            continue
        df = df.rename(columns={v: k for k, v in found.items()})

        vcp = detect_vcp(df)
        cup = detect_cup_and_handle(df)
        if not vcp and not cup:
            continue

        analysis.append({
            'symbol': symbol,
            'fin_met_count': fin_met_count,
            'vcp': vcp,
            'cup_handle': cup,
        })

    if not analysis:
        return pd.DataFrame()

    out_df = pd.DataFrame(analysis)
    out_df = out_df.sort_values(['vcp', 'cup_handle'], ascending=False)
    out_file = os.path.join(output_dir, 'pattern_analysis_results.csv')
    out_df.to_csv(out_file, index=False, encoding='utf-8-sig')
    out_df.to_json(out_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
    return out_df


def run_pattern_detection_on_financial_results() -> Optional[pd.DataFrame]:
    """advanced_financial_results.csv의 티커들에 대해 패턴 감지를 실행하고 결과를 저장
    
    Returns:
        Optional[pd.DataFrame]: 패턴 감지 결과 DataFrame 또는 결과가 없을 경우 None
    """
    import sys
    import os
    from datetime import datetime
    
    # 경로 설정 최적화
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    if current_dir not in sys.path:
        sys.path.append(current_dir)
    if project_root not in sys.path:
        sys.path.append(project_root)
    
    from config import ADVANCED_FINANCIAL_RESULTS_PATH, DATA_US_DIR
    from utils.io_utils import process_stock_data
    
    start_time = datetime.now()
    
    # advanced_financial_results.csv 읽기
    if not os.path.exists(ADVANCED_FINANCIAL_RESULTS_PATH):
        logger.error(f"❌ {ADVANCED_FINANCIAL_RESULTS_PATH} 파일이 존재하지 않습니다.")
        return None
    
    try:
        financial_df = pd.read_csv(ADVANCED_FINANCIAL_RESULTS_PATH)
        if financial_df.empty:
            logger.warning("❌ advanced_financial_results.csv가 비어있습니다.")
            return None
    except Exception as e:
        logger.error(f"❌ 파일 읽기 오류: {e}")
        return None
    
    logger.info(f"📊 {len(financial_df)} 개 종목에 대해 패턴 감지를 시작합니다...")
    print(f"📊 {len(financial_df)} 개 종목에 대해 패턴 감지를 시작합니다...")
    
    pattern_results = []
    processed_count = 0
    pattern_count = 0
    
    for idx, row in financial_df.iterrows():
        symbol = row['symbol']
        fin_met_count = row.get('fin_met_count', 0)
        processed_count += 1
        
        # 진행 상황 표시 (10% 단위로)
        if processed_count % max(1, len(financial_df) // 10) == 0:
            progress = processed_count / len(financial_df) * 100
            logger.info(f"진행 중: {progress:.1f}% 완료 ({processed_count}/{len(financial_df)})")
        
        try:
            # 로컬 CSV 파일에서 주가 데이터 읽기
            csv_file = f"{symbol}.csv"
            _, stock_data_full, _ = process_stock_data(csv_file, DATA_US_DIR, min_days=60, recent_days=365)
            
            if stock_data_full is None or len(stock_data_full) < 60:
                logger.debug(f"⚠️ {symbol}: 충분한 데이터가 없습니다.")
                continue
            
            # 최근 1년 데이터만 사용
            stock_data = stock_data_full.tail(365).copy()
            
            # date 컬럼을 인덱스로 설정
            if 'date' in stock_data.columns and not isinstance(stock_data.index, pd.DatetimeIndex):
                stock_data = stock_data.set_index('date')
            
            required_cols = ['open', 'high', 'low', 'close', 'volume']
            
            # 컬럼명 매핑 - 대소문자 구분 없이 처리
            col_mapping = {}
            for req_col in required_cols:
                for col in stock_data.columns:
                    if req_col.lower() == col.lower() or req_col.lower() in col.lower():
                        col_mapping[col] = req_col
                        break
            
            if len(col_mapping) < len(required_cols):
                missing_cols = set(required_cols) - set(col_mapping.values())
                logger.debug(f"⚠️ {symbol}: 필요한 컬럼이 부족합니다. 누락: {missing_cols}")
                continue
            
            # 컬럼명 변경
            stock_data = stock_data.rename(columns=col_mapping)
            
            # 패턴 감지
            vcp_detected = detect_vcp(stock_data)
            cup_detected = detect_cup_and_handle(stock_data)
            
            # 하나라도 만족하는 경우에만 결과에 추가
            if vcp_detected or cup_detected:
                pattern_count += 1
                pattern_results.append({
                    'symbol': symbol,
                    'fin_met_count': fin_met_count,
                    'rs_score': row.get('rs_score', None),
                    'rs_percentile': row.get('rs_percentile', None),
                    'fin_percentile': row.get('fin_percentile', None),
                    'total_percentile': row.get('total_percentile', None),
                    'vcp_pattern': vcp_detected,
                    'cup_handle_pattern': cup_detected,
                    'has_error': row.get('has_error', False),
                    'detection_date': datetime.now().strftime('%Y-%m-%d')
                })
                logger.info(f"✅ {symbol}: VCP={vcp_detected}, Cup&Handle={cup_detected}")
                print(f"✅ {symbol}: VCP={vcp_detected}, Cup&Handle={cup_detected}")
            
        except Exception as e:
            logger.error(f"⚠️ {symbol} 패턴 감지 중 오류: {str(e)}")
            continue
    
    # 실행 시간 계산
    elapsed_time = datetime.now() - start_time
    
    # 결과 저장
    if pattern_results:
        results_df = pd.DataFrame(pattern_results)
        
        # 정렬: VCP와 Cup&Handle 패턴 우선, 그 다음 total_percentile
        results_df['pattern_score'] = results_df['vcp_pattern'].astype(int) + results_df['cup_handle_pattern'].astype(int)
        results_df = results_df.sort_values(['pattern_score', 'total_percentile'], ascending=[False, False])
        results_df = results_df.drop('pattern_score', axis=1)
        
        # markminervini 폴더에 저장 (타임스탬프 없는 파일만 생성)
        output_dir = os.path.dirname(ADVANCED_FINANCIAL_RESULTS_PATH)
        csv_path = os.path.join(output_dir, 'pattern_detection_results.csv')
        json_path = os.path.join(output_dir, 'pattern_detection_results.json')
        
        try:
            results_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            results_df.to_json(json_path, orient='records', indent=2, force_ascii=False)
            
            logger.info(f"\n🎯 패턴 감지 완료: {len(results_df)}개 종목이 패턴을 만족합니다.")
            logger.info(f"📁 결과 저장: {csv_path}")
            logger.info(f"📁 결과 저장: {json_path}")
            
            print(f"\n🎯 패턴 감지 완료: {len(results_df)}개 종목이 패턴을 만족합니다.")
            print(f"📁 결과 저장: {csv_path}")
            
            # 상위 10개 결과 출력
            print("\n🏆 상위 10개 패턴 감지 결과:")
            top_10 = results_df.head(10)
            print(top_10[['symbol', 'fin_met_count', 'vcp_pattern', 'cup_handle_pattern', 'total_percentile']])
            
            # 실행 통계 출력
            print(f"\n⏱️ 실행 시간: {elapsed_time}")
            print(f"📊 처리된 종목 수: {processed_count}")
            print(f"✅ 패턴 감지된 종목 수: {pattern_count}")
            print(f"📈 패턴 감지 비율: {pattern_count/processed_count*100:.2f}%")
        except Exception as e:
            logger.error(f"결과 저장 중 오류 발생: {e}")
        
        return results_df
    else:
        logger.warning("❌ 패턴을 만족하는 종목이 없습니다.")
        print("❌ 패턴을 만족하는 종목이 없습니다.")
        
        # 빈 DataFrame이라도 컬럼 헤더와 함께 파일 생성
        empty_df = pd.DataFrame(columns=[
            'symbol', 'fin_met_count', 'rs_score', 'rs_percentile', 
            'fin_percentile', 'total_percentile', 'vcp_pattern', 
            'cup_handle_pattern', 'has_error', 'detection_date'
        ])
        
        # markminervini 폴더에 빈 파일 저장 (타임스탬프 없는 파일만 생성)
        output_dir = os.path.dirname(ADVANCED_FINANCIAL_RESULTS_PATH)
        csv_path = os.path.join(output_dir, 'pattern_detection_results.csv')
        json_path = os.path.join(output_dir, 'pattern_detection_results.json')
        
        try:
            empty_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            empty_df.to_json(json_path, orient='records', indent=2, force_ascii=False)
            
            print(f"📁 빈 결과 파일 생성: {csv_path}")
            
            # 실행 통계 출력
            print(f"\n⏱️ 실행 시간: {elapsed_time}")
            print(f"📊 처리된 종목 수: {processed_count}")
            print(f"✅ 패턴 감지된 종목 수: 0")
        except Exception as e:
            logger.error(f"결과 저장 중 오류 발생: {e}")
        
        return empty_df


def main():
    """메인 실행 함수"""
    try:
        run_pattern_detection_on_financial_results()
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}")
        print(f"❌ 오류 발생: {e}")
        return 1
    return 0


if __name__ == "__main__":
    main()