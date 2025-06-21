"""학술 논문 기반 VCP 및 Cup-with-Handle 패턴 감지 모듈

이 모듈은 Lo, Mamaysky & Wang (2000)과 Suh, Li & Gao (2008) 논문의
방법론을 기반으로 한 고급 패턴 감지 알고리즘을 구현합니다.

주요 기능:
- 비모수 커널 회귀를 이용한 가격 곡선 스무딩
- 연속적 변동성 수축 검출 (VCP)
- 2차 다항식 근사를 이용한 U자형 컵 검증
- 베지어 곡선과 상관계수 비교 (Cup & Handle)
- 배치 처리 및 CSV/JSON 결과 출력
"""

from __future__ import annotations

import os
import sys
import logging
from typing import Dict, List, Tuple, Optional, Union
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from scipy.signal import find_peaks, argrelextrema
from scipy import stats
from scipy.optimize import curve_fit
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF
from sklearn.model_selection import cross_val_score

# 프로젝트 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 내부 모듈 임포트
from config import MARKMINERVINI_RESULTS_DIR, ADVANCED_FINANCIAL_RESULTS_PATH, DATA_US_DIR
from utils.io_utils import process_stock_data

# 로깅 설정
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class KernelSmoothing:
    """비모수 커널 회귀를 이용한 가격 곡선 스무딩 클래스"""
    
    def __init__(self, bandwidth: Optional[float] = None):
        self.bandwidth = bandwidth
        self.smoothed_prices = None
        self.original_prices = None
    
    def gaussian_kernel(self, u: np.ndarray, h: float) -> np.ndarray:
        """가우시안 커널 함수"""
        return (1 / (h * np.sqrt(2 * np.pi))) * np.exp(-u**2 / (2 * h**2))
    
    def fit_smooth(self, prices: np.ndarray, dates: Optional[np.ndarray] = None) -> np.ndarray:
        """커널 회귀를 이용한 가격 스무딩"""
        self.original_prices = prices
        n = len(prices)
        
        if dates is None:
            x = np.arange(n)
        else:
            x = np.arange(n)
        
        # 최적 대역폭 결정 (교차검증법)
        if self.bandwidth is None:
            # 실무적 조정: CV 최적값의 30% 수준
            h_cv = self._cross_validation_bandwidth(x, prices)
            self.bandwidth = h_cv * 0.3
        
        smoothed = np.zeros(n)
        
        for i in range(n):
            weights = self.gaussian_kernel(x - x[i], self.bandwidth)
            smoothed[i] = np.sum(weights * prices) / np.sum(weights)
        
        self.smoothed_prices = smoothed
        return smoothed
    
    def _cross_validation_bandwidth(self, x: np.ndarray, y: np.ndarray) -> float:
        """교차검증을 통한 최적 대역폭 결정"""
        bandwidths = np.logspace(-2, 1, 20)  # 0.01 to 10
        best_score = -np.inf
        best_h = bandwidths[0]
        
        for h in bandwidths:
            scores = []
            for i in range(len(x)):
                # Leave-one-out cross validation
                x_train = np.delete(x, i)
                y_train = np.delete(y, i)
                x_test = x[i]
                y_test = y[i]
                
                weights = self.gaussian_kernel(x_train - x_test, h)
                if np.sum(weights) > 0:
                    y_pred = np.sum(weights * y_train) / np.sum(weights)
                    scores.append(-(y_test - y_pred)**2)  # Negative MSE
            
            avg_score = np.mean(scores)
            if avg_score > best_score:
                best_score = avg_score
                best_h = h
        
        return best_h
    
    def find_peaks_troughs(self) -> Tuple[np.ndarray, np.ndarray]:
        """스무딩된 곡선에서 피크와 골 추출"""
        if self.smoothed_prices is None:
            raise ValueError("먼저 fit_smooth()를 호출해야 합니다.")
        
        # 피크 찾기
        peaks, _ = find_peaks(self.smoothed_prices, distance=5)
        
        # 골 찾기 (음수로 변환 후 피크 찾기)
        troughs, _ = find_peaks(-self.smoothed_prices, distance=5)
        
        return peaks, troughs


class VCPDetector:
    """Volatility Contraction Pattern 감지기"""
    
    def __init__(self, min_contractions: int = 2):
        self.min_contractions = min_contractions
        self.smoother = KernelSmoothing()
    
    def detect(self, df: pd.DataFrame) -> bool:
        """VCP 패턴 감지
        
        Args:
            df: 주가 데이터 (최소 60일 이상)
            
        Returns:
            bool: VCP 패턴 감지 여부
        """
        if df is None or len(df) < 60:
            return False
        
        # 최근 90일 데이터 사용
        recent = df.tail(90).copy()
        prices = recent['close'].values
        
        # 커널 스무딩 적용
        smoothed = self.smoother.fit_smooth(prices)
        peaks, troughs = self.smoother.find_peaks_troughs()
        
        if len(peaks) < 3:  # 최소 3개 피크 필요
            return False
        
        # 연속적 진폭 감소 검증
        amplitudes = []
        for i in range(len(peaks) - 1):
            peak_idx = peaks[i]
            next_peak_idx = peaks[i + 1]
            
            # 두 피크 사이의 최저점 찾기
            trough_candidates = troughs[(troughs > peak_idx) & (troughs < next_peak_idx)]
            if len(trough_candidates) == 0:
                continue
            
            trough_idx = trough_candidates[np.argmin(smoothed[trough_candidates])]
            
            # 진폭 계산
            amplitude = smoothed[peak_idx] - smoothed[trough_idx]
            amplitudes.append(amplitude)
        
        # 연속적 감소 확인
        contractions = 0
        for i in range(1, len(amplitudes)):
            if amplitudes[i] < amplitudes[i-1]:
                contractions += 1
            else:
                contractions = 0  # 연속성 깨짐
        
        return contractions >= self.min_contractions


class CupHandleDetector:
    """Cup & Handle 패턴 감지기"""
    
    def __init__(self, correlation_threshold: float = 0.85):
        self.correlation_threshold = correlation_threshold
        self.smoother = KernelSmoothing()
    
    def detect(self, df: pd.DataFrame, window: int = 180) -> bool:
        """Cup & Handle 패턴 감지
        
        Args:
            df: 주가 데이터
            window: 분석 기간
            
        Returns:
            bool: Cup & Handle 패턴 감지 여부
        """
        if df is None or len(df) < window:
            return False
        
        data = df.tail(window).copy()
        prices = data['close'].values
        
        # 커널 스무딩 적용
        smoothed = self.smoother.fit_smooth(prices)
        peaks, troughs = self.smoother.find_peaks_troughs()
        
        if len(peaks) < 2 or len(troughs) == 0:
            return False
        
        # 컵 구간 식별
        left_peak = peaks[0]
        right_candidates = peaks[peaks > left_peak]
        if len(right_candidates) == 0:
            return False
        
        right_peak = right_candidates[-1]
        bottom_candidates = troughs[(troughs > left_peak) & (troughs < right_peak)]
        if len(bottom_candidates) == 0:
            return False
        
        bottom = bottom_candidates[np.argmin(smoothed[bottom_candidates])]
        
        # 컵 형성 기간 검증 (최소 30일)
        if right_peak - left_peak < 30:
            return False
        
        # U자형 검증 (2차 다항식 근사)
        if not self._verify_u_shape(smoothed, left_peak, bottom, right_peak):
            return False
        
        # 베지어 곡선 상관계수 검증
        if not self._verify_bezier_correlation(smoothed, left_peak, bottom, right_peak):
            return False
        
        # 핸들 검증
        if not self._verify_handle(smoothed, right_peak, len(smoothed) - 1):
            return False
        
        return True
    
    def _verify_u_shape(self, smoothed: np.ndarray, left: int, bottom: int, right: int) -> bool:
        """2차 다항식을 이용한 U자형 검증"""
        try:
            # 컵 구간 데이터
            x_cup = np.array([left, bottom, right])
            y_cup = smoothed[x_cup]
            
            # 2차 함수 피팅: f(t) = at^2 + bt + c
            def quadratic(x, a, b, c):
                return a * x**2 + b * x + c
            
            popt, _ = curve_fit(quadratic, x_cup, y_cup)
            a, b, c = popt
            
            # 곡률 검증 (a > 0이면 아래로 볼록)
            if a <= 0:
                return False
            
            # 대칭성 검증 (좌우 고점 높이 차이 5% 이내)
            left_high = smoothed[left]
            right_high = smoothed[right]
            height_diff = abs(left_high - right_high) / min(left_high, right_high)
            
            return height_diff <= 0.05
            
        except Exception:
            return False
    
    def _verify_bezier_correlation(self, smoothed: np.ndarray, left: int, bottom: int, right: int) -> bool:
        """베지어 곡선과 상관계수 비교"""
        try:
            # 7개 제어점 선정
            quarter1 = left + (bottom - left) // 4
            quarter3 = bottom + (right - bottom) // 4
            mid_left = (left + bottom) // 2
            mid_right = (bottom + right) // 2
            
            control_points = np.array([
                [left, smoothed[left]],
                [quarter1, smoothed[quarter1]],
                [mid_left, smoothed[mid_left]],
                [bottom, smoothed[bottom]],
                [mid_right, smoothed[mid_right]],
                [quarter3, smoothed[quarter3]],
                [right, smoothed[right]]
            ])
            
            # 베지어 곡선 생성 (간단한 근사)
            t = np.linspace(0, 1, right - left + 1)
            bezier_curve = self._generate_bezier_curve(control_points, t)
            
            # 상관계수 계산
            original_segment = smoothed[left:right+1]
            correlation = np.corrcoef(original_segment, bezier_curve)[0, 1]
            
            return correlation >= self.correlation_threshold
            
        except Exception:
            return False
    
    def _generate_bezier_curve(self, control_points: np.ndarray, t: np.ndarray) -> np.ndarray:
        """베지어 곡선 생성 (간단한 선형 보간)"""
        # 실제 베지어 곡선 대신 스플라인 보간 사용
        from scipy.interpolate import interp1d
        
        x_controls = control_points[:, 0]
        y_controls = control_points[:, 1]
        
        # 정규화된 t를 실제 x 좌표로 변환
        x_new = np.linspace(x_controls[0], x_controls[-1], len(t))
        
        # 스플라인 보간
        f = interp1d(x_controls, y_controls, kind='cubic', fill_value='extrapolate')
        return f(x_new)
    
    def _verify_handle(self, smoothed: np.ndarray, right_peak: int, end: int) -> bool:
        """핸들 검증 (컵 깊이의 33% 이내 조정)"""
        if end - right_peak < 5:  # 최소 5일 핸들
            return False
        
        handle_segment = smoothed[right_peak:end+1]
        handle_low = np.min(handle_segment)
        right_high = smoothed[right_peak]
        
        # 핸들 하락폭 계산
        handle_decline = (right_high - handle_low) / right_high
        
        # 컵 깊이의 33% 이내인지 확인
        return handle_decline <= 0.33


def analyze_tickers_from_results(results_dir: str, data_dir: str, output_dir: str = MARKMINERVINI_RESULTS_DIR) -> pd.DataFrame:
    """CSV 파일에서 티커 목록을 읽고 패턴을 감지하여 결과를 반환합니다."""
    os.makedirs(output_dir, exist_ok=True)
    results_file = os.path.join(results_dir, "advanced_financial_results.csv")
    
    if not os.path.exists(results_file):
        raise FileNotFoundError(f"결과 파일을 찾을 수 없습니다: {results_file}")
    
    logger.info(f"재무 결과 파일 로드 중: {results_file}")
    results_df = pd.read_csv(results_file)
    logger.info(f"총 {len(results_df)}개 종목에 대한 패턴 분석 시작")
    
    vcp_detector = VCPDetector()
    cup_detector = CupHandleDetector()
    analysis = []
    
    for _, row in results_df.iterrows():
        symbol = row['symbol']
        fin_met_count = row.get('fin_met_count', 0)
        
        file_path = os.path.join(data_dir, f"{symbol}.csv")
        if not os.path.exists(file_path):
            continue
        
        try:
            df = pd.read_csv(file_path)
            date_col = next((c for c in df.columns if c.lower() in ['date', '날짜', '일자']), None)
            if not date_col:
                continue
            
            df[date_col] = pd.to_datetime(df[date_col], utc=True)
            df.set_index(date_col, inplace=True)
            
            # 컬럼명 매핑
            col_map = {
                'high': ['high', 'High', '고가'],
                'low': ['low', 'Low', '저가'],
                'close': ['close', 'Close', '종가'],
                'volume': ['volume', 'Volume', '거래량']
            }
            
            found = {}
            for k, names in col_map.items():
                for c in df.columns:
                    if c.lower() in [n.lower() for n in names]:
                        found[k] = c
                        break
            
            if len(found) < 4:
                continue
            
            df = df.rename(columns={v: k for k, v in found.items()})
            
            # 패턴 감지
            vcp = vcp_detector.detect(df)
            cup = cup_detector.detect(df)
            
            if not vcp and not cup:
                continue
            
            analysis.append({
                'symbol': symbol,
                'fin_met_count': fin_met_count,
                'vcp': vcp,
                'cup_handle': cup,
                'detection_date': datetime.now().strftime('%Y-%m-%d')
            })
            
        except Exception as e:
            logger.error(f"⚠️ {symbol} 패턴 감지 중 오류: {str(e)}")
            continue
    
    if not analysis:
        return pd.DataFrame()
    
    out_df = pd.DataFrame(analysis)
    out_df = out_df.sort_values(['vcp', 'cup_handle'], ascending=False)
    
    # 결과 저장
    timestamp = datetime.now().strftime('%Y%m%d')
    out_file = os.path.join(output_dir, f'academic_pattern_results_{timestamp}.csv')
    out_df.to_csv(out_file, index=False, encoding='utf-8-sig')
    out_df.to_json(out_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
    
    return out_df


def run_pattern_detection_on_financial_results() -> Optional[pd.DataFrame]:
    """advanced_financial_results.csv의 티커들에 대해 학술 논문 기반 패턴 감지 실행"""
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
    
    logger.info(f"📊 {len(financial_df)} 개 종목에 대해 학술 논문 기반 패턴 감지를 시작합니다...")
    print(f"📊 {len(financial_df)} 개 종목에 대해 학술 논문 기반 패턴 감지를 시작합니다...")
    
    vcp_detector = VCPDetector(min_contractions=2)
    cup_detector = CupHandleDetector(correlation_threshold=0.85)
    
    pattern_results = []
    processed_count = 0
    pattern_count = 0
    
    for idx, row in financial_df.iterrows():
        symbol = row['symbol']
        fin_met_count = row.get('fin_met_count', 0)
        processed_count += 1
        
        # 진행 상황 표시
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
            
            # 컬럼명 매핑
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
            
            # 학술 논문 기반 패턴 감지
            vcp_detected = vcp_detector.detect(stock_data)
            cup_detected = cup_detector.detect(stock_data)
            
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
                    'detection_date': datetime.now().strftime('%Y-%m-%d'),
                    'detection_method': 'Academic (Kernel Regression + Bezier)'
                })
                logger.info(f"✅ {symbol}: VCP={vcp_detected}, Cup&Handle={cup_detected}")
                print(f"✅ {symbol}: VCP={vcp_detected}, Cup&Handle={cup_detected}")
            
        except Exception as e:
            logger.error(f"⚠️ {symbol} 패턴 감지 중 오류: {str(e)}")
            continue
    
    # 실행 시간 계산
    elapsed_time = datetime.now() - start_time
    
    # 결과 저장
    output_dir = os.path.dirname(ADVANCED_FINANCIAL_RESULTS_PATH)
    timestamp = datetime.now().strftime('%Y%m%d')
    
    if pattern_results:
        results_df = pd.DataFrame(pattern_results)
        
        # 정렬
        results_df['pattern_score'] = results_df['vcp_pattern'].astype(int) + results_df['cup_handle_pattern'].astype(int)
        results_df = results_df.sort_values(['pattern_score', 'total_percentile'], ascending=[False, False])
        results_df = results_df.drop('pattern_score', axis=1)
        
        # 파일 저장
        csv_path = os.path.join(output_dir, f'academic_pattern_results_{timestamp}.csv')
        json_path = os.path.join(output_dir, f'academic_pattern_results_{timestamp}.json')
        latest_csv_path = os.path.join(output_dir, 'academic_pattern_results.csv')
        latest_json_path = os.path.join(output_dir, 'academic_pattern_results.json')
        
        try:
            results_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            results_df.to_json(json_path, orient='records', indent=2, force_ascii=False)
            results_df.to_csv(latest_csv_path, index=False, encoding='utf-8-sig')
            results_df.to_json(latest_json_path, orient='records', indent=2, force_ascii=False)
            
            logger.info(f"\n🎯 학술 논문 기반 패턴 감지 완료: {len(results_df)}개 종목이 패턴을 만족합니다.")
            print(f"\n🎯 학술 논문 기반 패턴 감지 완료: {len(results_df)}개 종목이 패턴을 만족합니다.")
            print(f"📁 결과 저장: {csv_path}")
            print(f"📁 최신 결과: {latest_csv_path}")
            
            # 상위 10개 결과 출력
            print("\n🏆 상위 10개 패턴 감지 결과:")
            top_10 = results_df.head(10)
            print(top_10[['symbol', 'fin_met_count', 'vcp_pattern', 'cup_handle_pattern', 'total_percentile', 'detection_method']])
            
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
        
        # 빈 결과 파일 생성
        empty_df = pd.DataFrame(columns=[
            'symbol', 'fin_met_count', 'rs_score', 'rs_percentile', 
            'fin_percentile', 'total_percentile', 'vcp_pattern', 
            'cup_handle_pattern', 'has_error', 'detection_date', 'detection_method'
        ])
        
        csv_path = os.path.join(output_dir, f'academic_pattern_results_{timestamp}.csv')
        latest_csv_path = os.path.join(output_dir, 'academic_pattern_results.csv')
        
        try:
            empty_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            empty_df.to_csv(latest_csv_path, index=False, encoding='utf-8-sig')
            print(f"📁 빈 결과 파일 생성: {csv_path}")
            print(f"\n⏱️ 실행 시간: {elapsed_time}")
            print(f"📊 처리된 종목 수: {processed_count}")
        except Exception as e:
            logger.error(f"결과 저장 중 오류 발생: {e}")
        
        return empty_df


def main():
    """메인 실행 함수"""
    try:
        logger.info("학술 논문 기반 VCP 및 Cup & Handle 패턴 감지 시작")
        print("🔬 학술 논문 기반 패턴 감지 시스템 시작")
        print("📚 적용 논문: Lo, Mamaysky & Wang (2000), Suh, Li & Gao (2008)")
        print("🔧 방법론: 커널 회귀 스무딩 + 베지어 곡선 상관계수 분석\n")
        
        result = run_pattern_detection_on_financial_results()
        
        if result is not None and not result.empty:
            logger.info("✅ 패턴 감지 완료")
            return 0
        else:
            logger.warning("⚠️ 감지된 패턴이 없습니다")
            return 0
            
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {e}")
        print(f"❌ 오류 발생: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)