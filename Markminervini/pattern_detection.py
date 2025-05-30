# VCP 및 Cup-with-Handle 패턴 탐지 모듈
# 기계학습 없이 룰 기반으로 패턴을 식별하고 신뢰도 점수를 계산

import os
import pandas as pd
import numpy as np
from scipy.signal import find_peaks, argrelextrema
from scipy.stats import linregress, gaussian_kde
from statsmodels.nonparametric.kernel_regression import KernelReg
import warnings
from datetime import datetime, timedelta
import yfinance as yf
from sklearn.preprocessing import MinMaxScaler
import traceback
warnings.filterwarnings('ignore')

class ContractionAnalyzer:
    def __init__(self):
        pass
        
    def calculate_atr(self, high, low, close, length=14):
        """ATR(Average True Range) 계산"""
        try:
            tr = pd.DataFrame()
            tr['h-l'] = high - low
            tr['h-pc'] = abs(high - close.shift(1))
            tr['l-pc'] = abs(low - close.shift(1))
            tr['tr'] = tr[['h-l', 'h-pc', 'l-pc']].max(axis=1)
            return tr['tr'].rolling(length).mean()
        except Exception as e:
            print(f"ATR 계산 중 오류 발생: {e}")
            print(traceback.format_exc())
            return pd.Series()

    def analyze_contraction_signals(self, df):
        """5가지 수축 신호 분석"""
        try:
            vol = df['volume']
            high, low = df['high'], df['low']
            close = df['close']
            
            # ① VDU (Volume Dry-Up)
            v10 = vol.tail(10).mean()
            v50 = vol.ewm(span=50).mean().iloc[-1]
            vdu = v10 < 0.4 * v50
            
            # ② 가격 범위 수축
            range_now = (high - low).tail(5).mean()
            range_prev = (high - low).tail(10).head(5).mean()
            pr_contr = range_now < 0.8 * range_prev
            
            # ③ ATR 수축
            atr = self.calculate_atr(high, low, close)
            atr_contr = atr.iloc[-1] < 0.8 * atr.iloc[-15]
        
            # ④ 거래량 하락 추세
            y = np.log1p(vol.tail(20).values)
            slope, _ = np.polyfit(np.arange(20), y, 1)
            std_ratio = y.std() / y.mean()
            vol_down = (slope < -0.001) and (std_ratio < 0.2)
        
            # ⑤ Higher Lows
            lows = low.tail(3).values
            higher_lows = lows[0] < lows[1] < lows[2]
            
            # 점수 계산 (30점 만점)
            score = (
                5 * vdu +
                5 * pr_contr +
                5 * atr_contr +
                10 * vol_down +
                5 * higher_lows
            )
            
            return {
                'score': score,
                'signals': {
                    'VDU': vdu,
                    'PriceRange': pr_contr,
                    'ATRContract': atr_contr,
                    'VolDowntrend': vol_down,
                    'HigherLows': higher_lows
                }
            }
        except Exception as e:
            print(f"수축 신호 분석 중 오류 발생: {e}")
            print(traceback.format_exc())
            return {'score': 0, 'signals': {}}

def analyze_tickers_from_results(results_dir, data_dir, output_dir='../results2'):
    """advanced_financial_results.csv에서 티커를 가져와 수축 신호 분석을 수행합니다."""
    try:
        # results2 디렉토리 생성
        os.makedirs(output_dir, exist_ok=True)
        print(f"📁 결과 디렉토리 생성/확인: {output_dir}")
        
        # advanced_financial_results.csv 파일 존재 확인
        results_file = os.path.join(results_dir, 'advanced_financial_results.csv')
        if not os.path.exists(results_file):
            raise FileNotFoundError(f"결과 파일을 찾을 수 없습니다: {results_file}")
        
        results_df = pd.read_csv(results_file)
        analyzer = ContractionAnalyzer()
        analysis_results = []
        
        total_tickers = len(results_df)
        print(f"📊 {total_tickers}개 종목 분석 시작...")
        
        for idx, row in results_df.iterrows():
            try:
                symbol = row['symbol']
                fin_met_count = row['fin_met_count']
                rs_score = row['rs_score']
                
                # 진행 상황 표시
                if (idx + 1) % 100 == 0:
                    print(f"⏳ 진행 중: {idx + 1}/{total_tickers} 종목 처리됨")
                
                # fin_met_count가 5 미만인 경우 건너뛰기
                if fin_met_count < 5:
                    continue
                    
                file_path = os.path.join(data_dir, f'{symbol}.csv')
                if not os.path.exists(file_path):
                    print(f"⚠️ {symbol} 데이터 파일을 찾을 수 없습니다: {file_path}")
                    continue
                    
                df = pd.read_csv(file_path)
                
                # 컬럼명 확인 및 처리
                date_column = None
                for col in df.columns:
                    if col.lower() in ['date', '날짜', '일자']:
                        date_column = col
                        break
                
                if date_column is None:
                    print(f"⚠️ {symbol} 데이터에서 날짜 컬럼을 찾을 수 없습니다. 컬럼: {df.columns.tolist()}")
                    continue
                
                # 날짜 컬럼을 인덱스로 설정
                df[date_column] = pd.to_datetime(df[date_column])
                df.set_index(date_column, inplace=True)
                
                # 컬럼명 매핑 (대소문자 구분 없이)
                column_mapping = {
                    'high': ['high', 'high', '고가'],
                    'low': ['low', 'low', '저가'],
                    'close': ['close', 'close', '종가'],
                    'volume': ['volume', 'volume', '거래량']
                }
                
                # 컬럼명 찾기
                found_columns = {}
                for required_col, possible_names in column_mapping.items():
                    for col in df.columns:
                        if col.lower() in [name.lower() for name in possible_names]:
                            found_columns[required_col] = col
                            break
                
                # 필요한 컬럼이 모두 있는지 확인
                missing_columns = [col for col in column_mapping.keys() if col not in found_columns]
                if missing_columns:
                    print(f"⚠️ {symbol} 데이터에서 필요한 컬럼을 찾을 수 없습니다: {missing_columns}")
                    print(f"현재 컬럼: {df.columns.tolist()}")
                    continue
                
                # 컬럼명 변경
                df = df.rename(columns={v: k for k, v in found_columns.items()})
                
                result = analyzer.analyze_contraction_signals(df)
                
                # contraction_score가 5점 이하인 경우 건너뛰기
                if result['score'] <= 5:
                    continue
                
                analysis_results.append({
                    'symbol': symbol,
                    'rs_score': rs_score,
                    'contraction_score': result['score'],
                    'fin_met_count': fin_met_count,
                    'VDU': result['signals'].get('VDU', False),
                    'PriceRange': result['signals'].get('PriceRange', False),
                    'ATRContract': result['signals'].get('ATRContract', False),
                    'VolDowntrend': result['signals'].get('VolDowntrend', False),
                    'HigherLows': result['signals'].get('HigherLows', False)
                })
            except Exception as e:
                print(f"⚠️ {symbol} 분석 중 오류 발생: {e}")
                print(traceback.format_exc())
                continue
        
        if not analysis_results:
            print("❌ 분석 결과가 없습니다.")
            return pd.DataFrame()
        
        results_df = pd.DataFrame(analysis_results)
        # 수축 점수 기준으로 내림차순 정렬
        results_df = results_df.sort_values('contraction_score', ascending=False)
        
        # 결과 저장
        output_file = os.path.join(output_dir, 'pattern_analysis_results.csv')
        # 결과 저장
        results_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        # JSON 파일 생성 추가
        json_file = output_file.replace('.csv', '.json')
        results_df.to_json(json_file, orient='records', indent=2, force_ascii=False)
        print(f"✅ 분석 결과 저장 완료: {output_file}")
        
        return results_df
        
    except Exception as e:
        print(f"❌ 패턴 분석 중 오류 발생: {e}")
        print(traceback.format_exc())
        return pd.DataFrame()

# 중복 실행 방지를 위해 main 함수와 직접 실행 코드 제거
# def main():
#     """메인 실행 함수"""
#     try:
#         results_dir = '../results'
#         data_dir = '../data_us'
#         output_dir = '../results2'
#         
#         analyze_tickers_from_results(results_dir, data_dir, output_dir)
#         
#     except Exception as e:
#         print(f"❌ 메인 실행 중 오류 발생: {e}")
#         print(traceback.format_exc())

# if __name__ == "__main__":
#     main()