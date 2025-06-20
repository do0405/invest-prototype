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

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import RESULTS_VER2_DIR, OPTION_VOLATILITY_DIR
from utils import ensure_dir
from .skew_mixins import SkewCalculationsMixin

class VolatilitySkewScreener(SkewCalculationsMixin):
    """Xing et al.(2010) 논문 기반 변동성 스큐 역전 전략 스크리너"""
    
    def __init__(self, alpha_vantage_key: str = None):
        self.alpha_vantage_key = alpha_vantage_key
        self.target_stocks = self.get_large_cap_stocks()
        self.results_dir = OPTION_VOLATILITY_DIR
        os.makedirs(self.results_dir, exist_ok=True)
        
        # 데이터 품질 등급 정의
        self.data_quality_grades = {
            "alpha_vantage": {"grade": "A", "confidence_multiplier": 1.0, "description": "고품질 프리미엄 데이터"},
            "yfinance": {"grade": "B", "confidence_multiplier": 0.9, "description": "양호한 품질 무료 데이터"},
            "yfinance_fallback": {"grade": "C", "confidence_multiplier": 0.7, "description": "품질 부족하지만 사용 가능한 데이터"}
        }

    def get_large_cap_stocks(self) -> List[str]:
        """S&P 500 전체 종목 가져오기"""
        try:
            # S&P 500 구성 종목 가져오기
            sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
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
            
        except Exception as e:
            print(f"⚠️ S&P 500 목록 가져오기 실패: {e}")
            # 기본 대형주 목록 반환
            return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B', 'UNH', 'JNJ']
    
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
        
        # 방법 2: Alpha Vantage로 대체 시도 (yfinance 품질이 부족할 때만)
        if self.alpha_vantage_key:
            try:
                options_data = self.get_alpha_vantage_options(symbol)
                if self.validate_options_data(options_data):
                    print(f"✅ {symbol}: Alpha Vantage로 대체 성공")
                    return options_data, "alpha_vantage"
            except Exception as e:
                # Alpha Vantage 실패 시 (한도 초과 포함)
                if "rate limit" in str(e).lower() or "quota" in str(e).lower():
                    print(f"⚠️ {symbol}: Alpha Vantage 한도 초과, yfinance 데이터로 진행")
                else:
                    print(f"⚠️ {symbol}: Alpha Vantage 실패 ({e}), yfinance 데이터로 진행")
        
        # 방법 3: Alpha Vantage 실패 시 yfinance 데이터라도 사용
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

    
    def get_alpha_vantage_options(self, symbol: str) -> Optional[Dict]:
        """Alpha Vantage 옵션 API 활용"""
        if not self.alpha_vantage_key:
            return None
            
        url = "https://www.alphavantage.co/query"
        params = {
            'function': 'REALTIME_OPTIONS',
            'symbol': symbol,
            'apikey': self.alpha_vantage_key
        }
        
        response = requests.get(url, params=params)
        return response.json()
    
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
        
        # Alpha Vantage 데이터 검증
        if 'data' in data:
            options_list = data.get('data', [])
            if not options_list:
                return False
                
            has_atm_calls = any(
                0.95 <= float(opt.get('strike', 0)) / float(opt.get('underlying_price', 1)) <= 1.05
                for opt in options_list
                if opt.get('type') == 'call' and float(opt.get('implied_volatility', 0)) > 0
            )
            
            has_otm_puts = any(
                0.80 <= float(opt.get('strike', 0)) / float(opt.get('underlying_price', 1)) <= 0.95
                for opt in options_list
                if opt.get('type') == 'put' and float(opt.get('implied_volatility', 0)) > 0
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
        
        for i, symbol in enumerate(self.target_stocks, 1):
            print(f"\r진행률: {i}/{len(self.target_stocks)} ({i/len(self.target_stocks)*100:.1f}%) - 처리 중: {symbol}", end="")
            
            try:
                # 1단계: 기본 조건 체크
                if not self.meets_basic_criteria(symbol):
                    excluded_count += 1
                    continue
                
                # 2단계: 스큐 지수 계산 및 데이터 소스 확인
                skew, data_source = self.calculate_skew_index_with_source(symbol)
                
                if skew is None:
                    excluded_count += 1
                    continue
                
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
                    
                    # 품질 통계 업데이트
                    quality_stats[quality_grade] += 1
                    
                    results.append({
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
                    })
                    processed_count += 1
                else:
                    excluded_count += 1
                    
            except Exception as e:
                print(f"\n오류 발생 ({symbol}): {e}")
                excluded_count += 1
                continue
        
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
                description = list(self.data_quality_grades.values())[ord(grade) - ord('A')]['description']
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
        """결과를 CSV 파일로 저장"""
        if not results:
            return ""
        
        # DataFrame 생성
        df = pd.DataFrame(results)
        
        # 파일명 생성
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"volatility_skew_screening_{timestamp}.csv"
        filepath = os.path.join(self.results_dir, filename)
        
        # CSV 저장
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        # JSON 파일 생성 추가
        json_filepath = filepath.replace('.csv', '.json')
        df.to_json(json_filepath, orient='records', indent=2, force_ascii=False)
        
        print(f"\n💾 결과 저장 완료: {filepath}")
        return filepath
    
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
            
            return results, filepath
            
        except Exception as e:
            error_msg = f"스크리닝 실행 중 오류 발생: {e}"
            print(error_msg)
            print(traceback.format_exc())
            return [], ""


def run_volatility_skew_screening(alpha_vantage_key: Optional[str] = None) -> Tuple[List[Dict], str]:
    """변동성 스큐 스크리닝 실행 함수 (main.py에서 호출용)"""
    screener = VolatilitySkewScreener(alpha_vantage_key=alpha_vantage_key)
    return screener.run_screening()


if __name__ == "__main__":
    # 직접 실행 시 테스트
    print("🚀 변동성 스큐 역전 전략 테스트 실행")
    
    # Alpha Vantage API 키가 있다면 여기에 입력
    API_KEY = None  # "YOUR_ALPHA_VANTAGE_KEY"
    
    screener = VolatilitySkewScreener(alpha_vantage_key=API_KEY)
    portfolios, signals, portfolio_file, signals_file = screener.run_screening()
    
    if portfolios:
        print(f"\n✅ 포트폴리오 구성 완료")
        print(f"📁 포트폴리오 파일: {portfolio_file}")
        print(f"📁 신호 파일: {signals_file}")
    else:
        print("\n❌ 포트폴리오 구성에 실패했습니다.")
