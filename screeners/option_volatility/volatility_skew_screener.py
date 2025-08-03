# Xing et al.(2010) 기반 변동성 스큐 역전 전략 종목 스크리너

# -*- coding: utf-8 -*-

import os
import sys
import pandas as pd
import numpy as np
import yfinance as yf
import requests
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.path_utils import add_project_root

# 프로젝트 루트 디렉토리를 Python 경로에 추가
add_project_root()

from config import PORTFOLIO_RESULTS_DIR, OPTION_VOLATILITY_RESULTS_DIR
from utils import ensure_dir
from utils.screener_utils import save_screening_results, track_new_tickers, create_screener_summary
from screeners.option_volatility.skew_mixins import SkewCalculationsMixin

class VolatilitySkewScreener(SkewCalculationsMixin):
    """Xing et al.(2010) 논문 기반 변동성 스큐 역전 전략 스크리너"""

    def __init__(self):
        self.target_stocks = self.get_large_cap_stocks()
        self.results_dir = OPTION_VOLATILITY_RESULTS_DIR
        os.makedirs(self.results_dir, exist_ok=True)
        
        # 데이터 품질 등급 정의
        self.data_quality_grades = {
            "yfinance": {"grade": "B", "confidence_multiplier": 0.9, "description": "양호한 품질 무료 데이터"},
            "yfinance_fallback": {"grade": "C", "confidence_multiplier": 0.7, "description": "품질 부족하지만 사용 가능한 데이터"}
        }
        self.grade_description_map = {info["grade"]: info["description"] for info in self.data_quality_grades.values()}

    def get_large_cap_stocks(self) -> List[str]:
        """S&P 500 전체 종목 가져오기 (네트워크 오류 처리 강화)"""
        import time
        
        # 재시도 설정
        max_retries = 3
        retry_delay = 2  # 초
        
        for attempt in range(max_retries):
            try:
                print(f"📡 S&P 500 목록 가져오기 시도 {attempt + 1}/{max_retries}...")
                
                # S&P 500 구성 종목 가져오기
                sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
                
                # pandas read_html로 테이블 가져오기
                tables = pd.read_html(sp500_url)
                sp500_df = tables[0]
                symbols = sp500_df['Symbol'].tolist()
                
                # 일부 기호 정리 (예: BRK.B -> BRK-B)
                cleaned_symbols = []
                for symbol in symbols:
                    if '.' in symbol:
                        symbol = symbol.replace('.', '-')
                    cleaned_symbols.append(symbol)
                
                print(f"✅ S&P 500 구성 종목 {len(cleaned_symbols)}개 로드 완료")
                return cleaned_symbols  # 전체 종목 반환
                
            except requests.exceptions.RequestException as e:
                print(f"🌐 네트워크 오류 (시도 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    print(f"⏳ {retry_delay}초 후 재시도...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 지수 백오프
                else:
                    print(f"❌ 최대 재시도 횟수 초과. 기본 목록 사용")
            except Exception as e:
                print(f"⚠️ S&P 500 목록 가져오기 실패 (시도 {attempt + 1}/{max_retries}): {type(e).__name__}: {e}")
                if attempt < max_retries - 1:
                    print(f"⏳ {retry_delay}초 후 재시도...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    print(f"❌ 모든 재시도 실패. 기본 목록 사용")
        
        # 모든 시도 실패 시 기본 대형주 목록 반환
        default_stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B', 'UNH', 'JNJ', 
                         'V', 'JPM', 'WMT', 'PG', 'MA', 'HD', 'CVX', 'ABBV', 'BAC', 'KO']
        print(f"🔄 기본 대형주 목록 사용: {len(default_stocks)}개 종목")
        return default_stocks
    
    def get_options_data(self, symbol: str) -> Tuple[Optional[Dict], str]:
        """옵션 데이터 가져오기 - 유연한 하이브리드 접근법"""
        
        # 방법 1: yfinance 우선 시도 (무료, 빠름)
        yfinance_data = None
        try:
            yfinance_data = self.get_yfinance_options(symbol)
            if self.validate_options_data_quality(yfinance_data):  # 품질 검증 강화
                return yfinance_data, "yfinance"
            else:
                print(f"⚠️ {symbol}: yfinance 데이터 품질 부족")
        except Exception as e:
            print(f"⚠️ {symbol}: yfinance 실패 ({e})")
        
        # 방법 2: yfinance 데이터라도 사용
        if yfinance_data and self.validate_options_data(yfinance_data):
            print(f"📊 {symbol}: 품질은 낮지만 yfinance 데이터로 진행")
            return yfinance_data, "yfinance_fallback"
        
        # 방법 4: 모든 방법 실패 시에만 제외
        print(f"❌ {symbol}: 모든 데이터 소스 실패, 제외")
        return None, "excluded"

    def validate_options_data_quality(self, data: Optional[Dict]) -> bool:
        """yfinance 데이터 품질 검증 강화"""
        if not self.validate_options_data(data):
            return False
        
        # 추가 품질 검증
        try:
            calls = data.get('calls', [])
            puts = data.get('puts', [])
            
            # 1. 충분한 옵션 수량 확인
            if len(calls) < 5 or len(puts) < 5:
                return False
            
            # 2. IV 값의 합리성 확인 (0.05 ~ 2.0 범위)
            valid_calls = [c for c in calls if 0.05 <= c.get('impliedVolatility', 0) <= 2.0]
            valid_puts = [p for p in puts if 0.05 <= p.get('impliedVolatility', 0) <= 2.0]
            
            if len(valid_calls) < 3 or len(valid_puts) < 3:
                return False
            
            # 3. 거래량 확인 (최소한의 유동성)
            active_calls = [c for c in valid_calls if c.get('volume', 0) > 0]
            active_puts = [p for p in valid_puts if p.get('volume', 0) > 0]
            
            # 거래량이 없어도 IV가 있으면 허용 (품질은 낮지만 사용 가능)
            if len(active_calls) < 1 or len(active_puts) < 1:
                return False
            
            return True
            
        except Exception:
            return False

    
    
    def get_yfinance_options(self, symbol: str) -> Optional[Dict]:
        """yfinance 옵션 체인 활용"""
        try:
            ticker = yf.Ticker(symbol)
            current_price = ticker.history(period="1d")['Close'].iloc[-1]
            
            # 옵션 만기일 가져오기
            expirations = ticker.options
            if not expirations:
                return None
            
            # 10-60일 만기 필터링
            today = datetime.now().date()
            valid_expirations = []
            
            for exp_str in expirations:
                exp_date = datetime.strptime(exp_str, '%Y-%m-%d').date()
                days_to_exp = (exp_date - today).days
                if 10 <= days_to_exp <= 60:
                    valid_expirations.append(exp_str)
            
            if not valid_expirations:
                return None
            
            # 가장 가까운 만기 선택
            nearest_exp = valid_expirations[0]
            
            # 옵션 체인 가져오기
            opt_chain = ticker.option_chain(nearest_exp)
            calls = opt_chain.calls
            puts = opt_chain.puts
            
            # 데이터 구조화
            options_data = {
                'underlying_price': current_price,
                'expiration': nearest_exp,
                'calls': calls.to_dict('records'),
                'puts': puts.to_dict('records')
            }
            
            return options_data
            
        except Exception as e:
            print(f"yfinance 옵션 데이터 오류 ({symbol}): {e}")
            return None
    
    def validate_options_data(self, data: Optional[Dict]) -> bool:
        """옵션 데이터 품질 검증"""
        if not data:
            return False
        
        # yfinance 데이터 검증
        if 'calls' in data and 'puts' in data:
            calls = data['calls']
            puts = data['puts']
            underlying_price = data['underlying_price']
            
            # ATM 콜 존재 확인 (행사가/주가 비율 0.95~1.05)
            has_atm_calls = any(
                0.95 <= call['strike'] / underlying_price <= 1.05
                for call in calls
                if 'impliedVolatility' in call and call['impliedVolatility'] > 0
            )
            
            # OTM 풋 존재 확인 (행사가/주가 비율 0.80~0.95)
            has_otm_puts = any(
                0.80 <= put['strike'] / underlying_price <= 0.95
                for put in puts
                if 'impliedVolatility' in put and put['impliedVolatility'] > 0
            )
            
            return has_atm_calls and has_otm_puts
        
        return False
    
    def screen_stocks(self) -> List[Dict]:
        """전체 스크리닝 프로세스 - 데이터 품질 고려"""
        print("=== Xing et al.(2010) 변동성 스큐 역전 전략 스크리너 시작 ===")
        print(f"스크리닝 대상: {len(self.target_stocks)}개 종목")
        
        results = []
        excluded_count = 0
        processed_count = 0
        quality_stats = {"A": 0, "B": 0, "C": 0, "D": 0}
        
        def process_symbol(symbol):
            """개별 종목 처리 함수"""
            try:
                # 1단계: 기본 조건 체크
                if not self.meets_basic_criteria(symbol):
                    return None, 'excluded'
                
                # 2단계: 스큐 지수 계산 및 데이터 소스 확인
                skew, data_source = self.calculate_skew_index_with_source(symbol)
                
                if skew is None:
                    return None, 'excluded'
                
                # 3단계: 상승 후보 판별 (낮은 스큐)
                if self.is_bullish_candidate(symbol, skew):
                    # 회사명 가져오기
                    try:
                        ticker = yf.Ticker(symbol)
                        company_name = ticker.info.get('longName', symbol)
                    except:
                        company_name = symbol
                    
                    expected_return, base_confidence = self.estimate_expected_performance(skew)
                    
                    # 데이터 품질을 고려한 신뢰도 계산
                    adjusted_confidence, quality_grade, confidence_score = self.calculate_confidence_with_quality_adjustment(
                        symbol, skew, data_source
                    )
                    
                    result = {
                        'symbol': symbol,
                        'company_name': company_name,
                        'skew_index': skew,
                        'expected_return': expected_return,
                        'confidence_score': adjusted_confidence,
                        'base_confidence': base_confidence,
                        'data_source': data_source,
                        'data_quality_grade': quality_grade,
                        'confidence_numeric': confidence_score,
                        'quality_description': self.data_quality_grades[data_source]["description"]
                    }
                    return result, 'processed'
                else:
                    return None, 'excluded'
                    
            except Exception as e:
                print(f"\n오류 발생 ({symbol}): {e}")
                return None, 'error'
        
        # 병렬 처리 실행 (스레드 안전성 보장)
        max_workers = min(4, len(self.target_stocks))  # 최대 4개 워커
        completed_count = 0
        all_results = []  # 모든 결과를 임시로 저장
        temp_excluded_count = 0
        temp_processed_count = 0
        temp_quality_stats = {"A": 0, "B": 0, "C": 0, "D": 0}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 작업 제출
            future_to_symbol = {executor.submit(process_symbol, symbol): symbol for symbol in self.target_stocks}
            
            # 결과 수집 (스레드 안전)
            for future in as_completed(future_to_symbol):
                completed_count += 1
                symbol = future_to_symbol[future]
                
                # 진행률 출력
                print(f"\r진행률: {completed_count}/{len(self.target_stocks)} ({completed_count/len(self.target_stocks)*100:.1f}%) - 완료: {symbol}", end="")
                
                try:
                    result, status = future.result()
                    
                    if status == 'processed' and result is not None:
                        all_results.append(result)
                        temp_processed_count += 1
                        temp_quality_stats[result['data_quality_grade']] += 1
                    else:
                        temp_excluded_count += 1
                        
                except Exception as e:
                    print(f"\n{symbol} 결과 처리 중 오류: {e}")
                    temp_excluded_count += 1
        
        # 결과 병합 (메인 스레드에서 안전하게 처리)
        results.extend(all_results)
        excluded_count += temp_excluded_count
        processed_count += temp_processed_count
        for grade in quality_stats:
            quality_stats[grade] += temp_quality_stats[grade]
        
        print(f"\n\n스크리닝 완료: {processed_count}개 종목 선별, {excluded_count}개 종목 제외")
        print(f"데이터 품질 분포: A등급 {quality_stats['A']}개, B등급 {quality_stats['B']}개, C등급 {quality_stats['C']}개")
        
        # 스큐 지수 기준 오름차순 정렬 (낮은 스큐 = 높은 상승 가능성)
        results = sorted(results, key=lambda x: x['skew_index'])
        
        return results

    def generate_screening_report(self, results: List[Dict]) -> str:
        """데이터 품질 정보를 포함한 스크리닝 리포트 생성"""
        
        report = []
        report.append("=== Xing et al.(2010) 변동성 스큐 역전 전략 스크리너 ===")
        report.append(f"실행일: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        report.append(f"스크리닝 대상: {len(self.target_stocks)}개 종목")
        report.append("")
        
        if not results:
            report.append("❌ 조건을 만족하는 종목이 없습니다.")
            return "\n".join(report)
        
        # 상위 추천 종목 (낮은 스큐 = 높은 상승 가능성)
        report.append("📈 상승 유망 종목 TOP 10 (낮은 변동성 스큐 기준)")
        report.append("=" * 100)
        report.append(f"{'순위':<4} {'종목':<6} {'회사명':<18} {'스큐':<8} {'예상수익률':<10} {'신뢰도':<8} {'품질':<4} {'데이터소스':<12}")
        report.append("-" * 100)
        
        for i, stock in enumerate(results[:10], 1):
            company_name = stock['company_name'][:16] if len(stock['company_name']) > 16 else stock['company_name']
            data_source_display = stock['data_source'].replace('_fallback', '*')
            
            # 품질이 낮은 경우 표시
            quality_indicator = f"{stock['data_quality_grade']}"
            if stock['data_source'] == 'yfinance_fallback':
                quality_indicator += "⚠️"
            
            report.append(
                f"{i:<4} {stock['symbol']:<6} {company_name:<18} "
                f"{stock['skew_index']:<8.2f}% {stock['expected_return']:<10.1%} {stock['confidence_score']:<8} "
                f"{quality_indicator:<4} {data_source_display:<12}"
            )
        
        report.append("")
        report.append("📊 스크리닝 통계")
        report.append(f"• 기본 조건 통과: {len(results)}개 종목")
        report.append(f"• 낮은 스큐 (상승 유망): {len([r for r in results if r['skew_index'] < 4.76])}개")
        report.append(f"• 높은 스큐 (주의 필요): {len([r for r in results if r['skew_index'] > 8.43])}개")
        
        # 데이터 품질 통계
        quality_counts = {}
        for result in results:
            grade = result['data_quality_grade']
            quality_counts[grade] = quality_counts.get(grade, 0) + 1
        
        report.append("")
        report.append("📋 데이터 품질 분포")
        for grade in ['A', 'B', 'C']:
            count = quality_counts.get(grade, 0)
            if count > 0:
                description = self.grade_description_map.get(grade, "")
                report.append(f"• {grade}등급: {count}개 종목 ({description})")
        
        # 품질 경고
        fallback_count = len([r for r in results if r['data_source'] == 'yfinance_fallback'])
        if fallback_count > 0:
            report.append("")
            report.append("⚠️ 데이터 품질 주의사항")
            report.append(f"• {fallback_count}개 종목이 품질 부족 데이터로 분석됨 (신뢰도 하향 조정)")
            report.append("• 해당 종목들은 추가 검증 권장")
        
        # 논문 근거 설명
        report.append("")
        report.append("📋 전략 근거 (Xing et al. 2010)")
        report.append("• 낮은 스큐 종목이 높은 스큐 종목보다 연간 10.9% 높은 수익률")
        report.append("• 예측력은 최소 6개월간 지속")
        report.append("• 높은 스큐 = 나쁜 실적 서프라이즈 예상")
        
        return "\n".join(report)

    def save_results(self, results: List[Dict]) -> str:
        """결과를 CSV 파일로 저장 (날짜만 포함한 파일명 사용)"""
        if not results:
            return ""
        
        # 결과 저장 (날짜만 포함한 파일명 사용)
        results_paths = save_screening_results(
            results=results,
            output_dir=self.results_dir,
            filename_prefix="volatility_skew_screening",
            include_timestamp=True,
            incremental_update=True
        )
        
        return results_paths['csv_path']
    
    def run_screening(self, save_results: bool = True) -> Tuple[List[Dict], str]:
        """전체 스크리닝 실행"""
        try:
            # 스크리닝 실행
            results = self.screen_stocks()
            
            # 리포트 생성
            report = self.generate_screening_report(results)
            print("\n" + report)
            
            # 결과 저장
            filepath = ""
            if save_results and results:
                filepath = self.save_results(results)
                
                # 새로운 티커 추적
                tracker_file = os.path.join(self.results_dir, "new_volatility_skew_tickers.csv")
                new_tickers = track_new_tickers(
                    current_results=results,
                    tracker_file=tracker_file,
                    symbol_key='symbol',
                    retention_days=14
                )
                
                # 요약 정보 생성
                summary = create_screener_summary(
                    screener_name="Volatility Skew",
                    total_candidates=len(results),
                    new_tickers=len(new_tickers),
                    results_paths={'csv': filepath, 'json': filepath.replace('.csv', '.json') if filepath else ''}
                )
                
                print(f"✅ 변동성 스큐 스크리닝 완료: {len(results)}개 종목, 신규 {len(new_tickers)}개")
            
            return results, filepath
            
        except Exception as e:
            error_msg = f"스크리닝 실행 중 오류 발생: {e}"
            print(error_msg)
            print(traceback.format_exc())
            return [], ""


def run_volatility_skew_screening() -> Tuple[List[Dict], str]:
    """변동성 스큐 스크리닝 실행 함수 (main.py에서 호출용)"""
    screener = VolatilitySkewScreener()
    return screener.run_screening()
