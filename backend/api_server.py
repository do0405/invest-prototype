# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv
from flask import Flask, jsonify, request
from flask_cors import CORS

# 환경변수 로드
load_dotenv()
import pandas as pd
import sys
import glob
import json  # 추가된 import
from datetime import datetime

import sys
import os
import math
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.path_utils import add_project_root

# 프로젝트 루트 디렉토리를 Python 경로에 추가
add_project_root()

# NaN 값을 처리하는 헬퍼 함수
def sanitize_json_data(data):
    """JSON 직렬화를 위해 NaN, Infinity 값을 처리"""
    if isinstance(data, dict):
        return {k: sanitize_json_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [sanitize_json_data(item) for item in data]
    elif isinstance(data, float):
        if math.isnan(data) or math.isinf(data):
            return 0.0
        return data
    else:
        return data

from config import (
    RESULTS_DIR,
    PORTFOLIO_RESULTS_DIR,
    MARKMINERVINI_RESULTS_DIR,
    IPO_INVESTMENT_RESULTS_DIR,
    LEADER_STOCK_RESULTS_DIR,
    MOMENTUM_SIGNALS_RESULTS_DIR,
    US_SETUP_RESULTS_DIR,
    US_GAINER_RESULTS_DIR,
    MARKET_REGIME_DIR,
)
from typing import Optional

app = Flask(__name__)
CORS(app, origins=['http://localhost:3001', 'http://127.0.0.1:3001'], 
     methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
     allow_headers=['Content-Type', 'Authorization'])  # CORS 허용

@app.route('/api/screening-results', methods=['GET'])
def get_screening_results():
    """스크리닝 결과 반환"""
    try:
        # us_with_rs.json 파일 읽기
        json_file = os.path.join(MARKMINERVINI_RESULTS_DIR, 'us_with_rs.json')
        if os.path.exists(json_file):
            df = pd.read_json(json_file)
            # NaN 값을 None으로 변환
            df = df.where(pd.notnull(df), None)
            mtime = os.path.getmtime(json_file)
            return jsonify({
                'success': True,
                'data': df.to_dict('records'),
                'total_count': len(df),
                'last_updated': datetime.fromtimestamp(mtime).isoformat()
            })
        else:
            return jsonify({'success': False, 'message': 'Data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/financial-results', methods=['GET'])
def get_financial_results():
    """재무제표 스크리닝 결과 반환"""
    try:
        json_file = os.path.join(MARKMINERVINI_RESULTS_DIR, 'advanced_financial_results.json')
        if os.path.exists(json_file):
            df = pd.read_json(json_file)
            # NaN 값을 None으로 변환
            df = df.where(pd.notnull(df), None)
            mtime = os.path.getmtime(json_file)
            return jsonify({
                'success': True,
                'data': df.to_dict('records'),
                'total_count': len(df),
                'last_updated': datetime.fromtimestamp(mtime).isoformat()
            })
        else:
            return jsonify({'success': False, 'message': 'Data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/integrated-results', methods=['GET'])
def get_integrated_results():
    """통합 스크리닝 결과 반환"""
    try:
        json_file = os.path.join(MARKMINERVINI_RESULTS_DIR, 'integrated_results.json')
        if os.path.exists(json_file):
            df = pd.read_json(json_file)
            # NaN 값을 None으로 변환
            df = df.where(pd.notnull(df), None)
            mtime = os.path.getmtime(json_file)
            return jsonify({
                'success': True,
                'data': df.to_dict('records'),
                'total_count': len(df),
                'last_updated': datetime.fromtimestamp(mtime).isoformat()
            })
        else:
            return jsonify({'success': False, 'message': 'Data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/portfolio/<strategy_name>', methods=['GET'])
def get_portfolio_by_strategy(strategy_name):
    """전략별 포트폴리오 결과 반환 (실시간 가격 및 수익률 업데이트)"""
    try:
        # Check in buy directory first
        json_file = os.path.join(PORTFOLIO_RESULTS_DIR, 'buy', f'{strategy_name}_results.json')
        if not os.path.exists(json_file):
            # Check in sell directory
            json_file = os.path.join(PORTFOLIO_RESULTS_DIR, 'sell', f'{strategy_name}_results.json')
        
        if os.path.exists(json_file):
            df = pd.read_json(json_file)
            # NaN 값을 None으로 변환
            df = df.where(pd.notnull(df), None)
            
            # 실시간 가격 및 수익률 업데이트
            updated_data = []
            for _, row in df.iterrows():
                item = row.to_dict()
                symbol = item.get('symbol', item.get('종목명', ''))
                
                if symbol and symbol != 'N/A':
                    try:
                        import yfinance as yf
                        ticker = yf.Ticker(symbol)
                        hist = ticker.history(period='1d')
                        
                        if not hist.empty:
                            current_price = float(hist['Close'].iloc[-1])
                            
                            # 진입가 업데이트 (시장가인 경우)
                            entry_price = item.get('매수가', item.get('entry_price', 0))
                            if entry_price == '시장가' or entry_price == 'N/A' or entry_price is None:
                                item['매수가'] = round(current_price, 2)
                                item['entry_price'] = round(current_price, 2)
                                entry_price = current_price
                            else:
                                entry_price = float(entry_price) if entry_price != 0 else current_price
                            
                            # 수익률 계산
                            if entry_price and entry_price > 0:
                                profit_rate = ((current_price - entry_price) / entry_price) * 100
                                item['수익률'] = round(profit_rate, 2)
                                item['profit_rate'] = round(profit_rate, 2)
                            
                            # 현재가 업데이트
                            item['현재가'] = round(current_price, 2)
                            item['current_price'] = round(current_price, 2)
                            
                    except Exception as e:
                        print(f"Error updating price for {symbol}: {e}")
                        # 실패 시 기존 값 유지
                        pass
                
                updated_data.append(item)
            
            return jsonify({
                'success': True,
                'strategy': strategy_name,
                'data': updated_data,
                'total_count': len(updated_data)
            })
        else:
            return jsonify({'success': False, 'message': f'{strategy_name} data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/strategy-description/<strategy_name>', methods=['GET'])
def get_strategy_description(strategy_name):
    """전략 설명 텍스트 반환"""
    try:
        # 프로젝트 루트 디렉토리 기준으로 절대경로 설정
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        md_path = os.path.join(project_root, 'portfolio', 'long_short', 'strategy', f'{strategy_name}.md')
        if os.path.exists(md_path):
            with open(md_path, 'r', encoding='utf-8') as f:
                text = f.read()
            return jsonify({'success': True, 'data': text})
        return jsonify({'success': False, 'message': 'Description not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/screener-description/<screener_name>', methods=['GET'])
def get_screener_description(screener_name):
    """스크리너 설명 텍스트 반환"""
    try:
        # 프로젝트 루트 디렉토리 기준으로 절대경로 설정
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        md_path = os.path.join(project_root, 'screeners', 'markminervini', f'{screener_name}.md')
        if os.path.exists(md_path):
            with open(md_path, 'r', encoding='utf-8') as f:
                text = f.read()
            return jsonify({'success': True, 'data': text})
        return jsonify({'success': False, 'message': 'Description not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/volatility-skew', methods=['GET'])
def get_volatility_skew_results():
    """변동성 스큐 스크리닝 결과 반환"""
    try:
        # 가장 최근 파일 찾기 (날짜 형식 파일명 지원)
        pattern = os.path.join(RESULTS_DIR, 'screeners', 'option_volatility', 'volatility_skew_screening_*.json')
        files = glob.glob(pattern)
        if files:
            latest_file = max(files, key=os.path.getctime)
            df = pd.read_json(latest_file)
            # NaN 값을 None으로 변환
            df = df.where(pd.notnull(df), None)
            mtime = os.path.getmtime(latest_file)
            return jsonify({
                'success': True,
                'data': df.to_dict('records'),
                'total_count': len(df),
                'last_updated': datetime.fromtimestamp(mtime).isoformat()
            })
        else:
            return jsonify({'success': False, 'message': 'Volatility skew data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# --- New screener result endpoints ---

def _load_latest_json(directory: str, filename_prefix: str = None) -> tuple[Optional[pd.DataFrame], Optional[float]]:
    """안전한 최신 JSON 파일 로딩 (개선된 파일 매칭 로직)
    
    Args:
        directory: 검색할 디렉토리
        filename_prefix: 파일명 접두사 (None이면 모든 JSON 파일)
    
    Returns:
        (DataFrame, modification_time) 또는 (None, None)
    """
    def _extract_timestamp_from_filename(filename: str) -> Optional[datetime]:
        """파일명에서 타임스탬프 추출 (_YYYYMMDD 또는 _YYYY-MM-DD 형식)"""
        import re
        # _YYYYMMDD 형식 매칭
        match = re.search(r'_(\d{8})(?:\.json)?$', filename)
        if match:
            try:
                return datetime.strptime(match.group(1), '%Y%m%d')
            except ValueError:
                pass
        
        # _YYYY-MM-DD 형식 매칭
        match = re.search(r'_(\d{4}-\d{2}-\d{2})(?:\.json)?$', filename)
        if match:
            try:
                return datetime.strptime(match.group(1), '%Y-%m-%d')
            except ValueError:
                pass
        
        return None
    
    def _get_file_priority(file_path: str, filename_prefix: str) -> tuple[int, datetime]:
        """파일 우선순위 계산 (타임스탬프 > 파일 시스템 시간)"""
        filename = os.path.basename(file_path)
        
        # 1. 파일명에서 타임스탬프 추출 시도
        timestamp = _extract_timestamp_from_filename(filename)
        if timestamp:
            return (1, timestamp)  # 높은 우선순위
        
        # 2. 파일 시스템 수정 시간 사용
        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
        return (0, file_mtime)  # 낮은 우선순위
    
    if filename_prefix:
        # 정확한 접두사 매칭
        pattern = os.path.join(directory, f'{filename_prefix}*.json')
    else:
        pattern = os.path.join(directory, '*.json')
    
    files = glob.glob(pattern)
    if not files:
        print(f"[API] 파일을 찾을 수 없음: {pattern}")
        return None, None
    
    # 파일명 검증 (접두사가 정확히 일치하는지 확인)
    if filename_prefix:
        validated_files = []
        for file_path in files:
            filename = os.path.basename(file_path)
            # 접두사로 시작하고, 그 다음이 '_', '.', 또는 파일 끝인지 확인
            if (filename.startswith(filename_prefix) and 
                (len(filename) == len(filename_prefix) + 5 or  # .json
                 filename[len(filename_prefix)] in ['_', '.'])):
                validated_files.append(file_path)
        files = validated_files
    
    if not files:
        print(f"[API] 검증된 파일이 없음: {filename_prefix}")
        return None, None
    
    # 우선순위 기준으로 최신 파일 선택 (타임스탬프 > 파일 시스템 시간)
    latest = max(files, key=lambda f: _get_file_priority(f, filename_prefix or ''))
    
    try:
        df = pd.read_json(latest)
        # NaN 값을 None으로 변환
        df = df.where(pd.notnull(df), None)
        print(f"[API] 로드된 파일: {os.path.basename(latest)} (크기: {len(df)}행)")
        return df, os.path.getmtime(latest)
    except Exception as e:
        print(f"[API] JSON 파일 로드 실패: {latest}, 오류: {e}")
        return None, None


@app.route('/api/ipo-investment', methods=['GET'])
def get_ipo_investment_results():
    """Return latest IPO investment screener results."""
    try:
        df, mtime = _load_latest_json(IPO_INVESTMENT_RESULTS_DIR, 'ipo_investment_results')
        if df is not None:
            return jsonify({'success': True, 'data': df.to_dict('records'), 'total_count': len(df),
                            'last_updated': datetime.fromtimestamp(mtime).isoformat() if mtime else None})
        return jsonify({'success': False, 'message': 'IPO data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/leader-stock', methods=['GET'])
def get_leader_stock_results():
    """Return latest leader stock screener results."""
    try:
        # market_reversal_leaders 우선, 없으면 leader_stock_results
        df, mtime = _load_latest_json(LEADER_STOCK_RESULTS_DIR, 'market_reversal_leaders')
        if df is None:
            df, mtime = _load_latest_json(LEADER_STOCK_RESULTS_DIR, 'leader_stock_results')
        
        if df is not None:
            return jsonify({'success': True, 'data': df.to_dict('records'), 'total_count': len(df),
                            'last_updated': datetime.fromtimestamp(mtime).isoformat() if mtime else None})
        return jsonify({'success': False, 'message': 'Leader stock data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/momentum-signals', methods=['GET'])
def get_momentum_signals_results():
    """Return latest momentum signals screener results."""
    try:
        # momentum_signals 우선, 없으면 stage2_breakouts (하위 호환성)
        df, mtime = _load_latest_json(MOMENTUM_SIGNALS_RESULTS_DIR, 'momentum_signals')
        if df is None:
            df, mtime = _load_latest_json(MOMENTUM_SIGNALS_RESULTS_DIR, 'stage2_breakouts')
        if df is None:
            df, mtime = _load_latest_json(MOMENTUM_SIGNALS_RESULTS_DIR, 'momentum_signals_results')
        
        if df is not None:
            # 데이터 매핑 및 변환
            data = df.to_dict('records')
            for item in data:
                # ticker를 symbol로 매핑
                if 'ticker' in item and 'symbol' not in item:
                    item['symbol'] = item['ticker']
                
                # date를 signal_date로 매핑
                if 'date' in item and 'signal_date' not in item:
                    item['signal_date'] = item['date']
                
                # close를 price로도 매핑
                if 'close' in item and 'price' not in item:
                    item['price'] = item['close']
                
                # Pattern detection field mappings for backward compatibility
                if 'vcp_detected' in item and 'VCP_Pattern' not in item:
                    item['VCP_Pattern'] = item['vcp_detected']
                if 'cup_handle_detected' in item and 'Cup_Handle_Pattern' not in item:
                    item['Cup_Handle_Pattern'] = item['cup_handle_detected']
                if 'Symbol' in item and 'symbol' not in item:
                    item['symbol'] = item['Symbol']
            
            # NaN 값 처리
            sanitized_data = sanitize_json_data(data)
            
            return jsonify({'success': True, 'data': sanitized_data, 'total_count': len(sanitized_data),
                            'last_updated': datetime.fromtimestamp(mtime).isoformat() if mtime else None})
        return jsonify({'success': False, 'message': 'Momentum signals data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/us-setup', methods=['GET'])
def get_us_setup_results():
    """Return latest US Setup screener results."""
    try:
        df, mtime = _load_latest_json(US_SETUP_RESULTS_DIR, 'us_setup_results')
        if df is not None:
            return jsonify({'success': True, 'data': df.to_dict('records'), 'total_count': len(df),
                            'last_updated': datetime.fromtimestamp(mtime).isoformat() if mtime else None})
        return jsonify({'success': False, 'message': 'US Setup data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/us-gainers', methods=['GET'])
def get_us_gainers_results():
    """Return latest US Gainers screener results."""
    try:
        df, mtime = _load_latest_json(US_GAINER_RESULTS_DIR, 'us_gainers_results')
        if df is not None:
            return jsonify({'success': True, 'data': df.to_dict('records'), 'total_count': len(df),
                            'last_updated': datetime.fromtimestamp(mtime).isoformat() if mtime else None})
        return jsonify({'success': False, 'message': 'US Gainers data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/market-regime', methods=['GET'])
def get_market_regime_latest():
    """Return latest market regime analysis result."""
    try:
        latest_file = os.path.join(MARKET_REGIME_DIR, 'latest_market_regime.json')
        if os.path.exists(latest_file):
            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify({'success': True, 'data': data})
        return jsonify({'success': False, 'message': 'Market regime data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/run-screening', methods=['POST'])
def run_screening():
    """스크리닝 실행 (기존 main.py 호출)"""
    try:
        import subprocess
        data = request.get_json()
        mode = data.get('mode', 'integrated')  # rs-only, financial-only, integrated 등
        
        # main.py 실행
        cmd = ['python', 'main.py', f'--{mode}']
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        return jsonify({
            'success': result.returncode == 0,
            'message': 'Screening completed' if result.returncode == 0 else 'Screening failed',
            'output': result.stdout,
            'error': result.stderr if result.returncode != 0 else None
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/markminervini/<screener_name>', methods=['GET'])
def get_markminervini_results(screener_name):
    """Markminervini 스크리너 결과 반환"""
    try:
        # Check markminervini results directory first
        json_file = os.path.join(MARKMINERVINI_RESULTS_DIR, f'{screener_name}.json')
        
        # If not found, check main results directory
        if not os.path.exists(json_file):
            json_file = os.path.join(RESULTS_DIR, f'{screener_name}.json')
        
        # Special handling for pattern results - CSV 파일을 JSON으로 변환 with field mapping
        if not os.path.exists(json_file) and screener_name in ['image_pattern_results', 'integrated_pattern_results', 'integrated_results']:
            # CSV 파일 경로 확인
            csv_file = os.path.join(MARKMINERVINI_RESULTS_DIR, f'{screener_name}.csv')
            if os.path.exists(csv_file):
                try:
                    # CSV를 읽어서 JSON 형태로 변환
                    df = pd.read_csv(csv_file)
                    # NaN 값을 None으로 변환하여 JSON 직렬화 문제 해결
                    df = df.where(pd.notnull(df), None)
                    
                    # numpy 타입을 Python 기본 타입으로 변환
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            df[col] = df[col].astype(str).replace('nan', None)
                        elif 'float' in str(df[col].dtype):
                            df[col] = df[col].astype(float)
                        elif 'int' in str(df[col].dtype):
                            df[col] = df[col].astype(int)
                    
                    data = df.to_dict('records')
                    
                    # 추가적으로 numpy 값과 NaN 문자열 정리
                    import re
                    import math
                    import json as json_module
                    for item in data:
                        for key, value in item.items():
                            if isinstance(value, str):
                                # numpy 표현식, NaN 문자열, dimensional_scores 문제 해결
                                if ('np.float64(' in value or 'NaN' in value or value == 'nan' or 
                                    'dimensional_scores' in key):
                                    try:
                                        # dimensional_scores 필드의 특별 처리
                                        if 'dimensional_scores' in key:
                                            # numpy 표현식을 포함한 문자열을 딕셔너리로 변환
                                            cleaned_value = re.sub(r'np\.float64\(([0-9]*\.?[0-9]+)\)', r'\1', value)
                                            cleaned_value = cleaned_value.replace("'", '"')
                                            try:
                                                item[key] = json_module.loads(cleaned_value)
                                            except:
                                                item[key] = None
                                        else:
                                            # 일반적인 numpy 표현식에서 숫자 추출
                                            match = re.search(r'([0-9]*\.?[0-9]+)', value)
                                            if match:
                                                num_val = float(match.group(1))
                                                # NaN이나 무한대 값 체크
                                                if math.isnan(num_val) or math.isinf(num_val):
                                                    item[key] = None
                                                else:
                                                    item[key] = num_val
                                            else:
                                                item[key] = None
                                    except:
                                        item[key] = None
                            elif isinstance(value, float):
                                # float 값에서 NaN이나 무한대 체크
                                if math.isnan(value) or math.isinf(value):
                                    item[key] = None
                    
                    # Apply field mapping based on screener type
                    mapped_data = []
                    for item in data:
                        mapped_item = dict(item)
                        
                        # Common field mappings
                        if 'detection_date' in mapped_item and 'signal_date' not in mapped_item:
                            mapped_item['signal_date'] = mapped_item['detection_date']
                        if 'processing_date' in mapped_item and 'signal_date' not in mapped_item:
                            mapped_item['signal_date'] = mapped_item['processing_date']
                        if 'fin_met_count' in mapped_item and 'met_count' not in mapped_item:
                            mapped_item['met_count'] = mapped_item['fin_met_count']
                        
                        # Pattern detection field mappings for backward compatibility
                        if 'vcp_detected' in mapped_item and 'VCP_Pattern' not in mapped_item:
                            mapped_item['VCP_Pattern'] = mapped_item['vcp_detected']
                        if 'cup_handle_detected' in mapped_item and 'Cup_Handle_Pattern' not in mapped_item:
                            mapped_item['Cup_Handle_Pattern'] = mapped_item['cup_handle_detected']
                        if 'Symbol' in mapped_item and 'symbol' not in mapped_item:
                            mapped_item['symbol'] = mapped_item['Symbol']
                            
                        # 스크리너별 특별 처리
                        should_include = True
                        
                        if screener_name == 'image_pattern_results':
                            # 이미지 패턴 결과: VCP 또는 Cup&Handle 패턴이 감지된 종목만 포함
                            vcp_detected = mapped_item.get('vcp_detected', False)
                            cup_handle_detected = mapped_item.get('cup_handle_detected', False)
                            
                            # Boolean 값으로 변환 (문자열 'true'/'false'도 처리)
                            if isinstance(vcp_detected, str):
                                vcp_detected = vcp_detected.lower() == 'true'
                            if isinstance(cup_handle_detected, str):
                                cup_handle_detected = cup_handle_detected.lower() == 'true'
                                
                            should_include = bool(vcp_detected) or bool(cup_handle_detected)
                            
                        elif screener_name == 'integrated_pattern_results':
                            # 통합 패턴 결과: High confidence level을 가진 종목만 포함, 제한된 컬럼만 표시
                            vcp_confidence_level = mapped_item.get('vcp_confidence_level', '')
                            cup_handle_confidence_level = mapped_item.get('cup_handle_confidence_level', '')
                            should_include = vcp_confidence_level == 'High' or cup_handle_confidence_level == 'High'
                            
                            if should_include:
                                # 제한된 컬럼만 포함
                                filtered_item = {
                                    'symbol': mapped_item.get('symbol', ''),
                                    'date': mapped_item.get('processing_date', ''),
                                    'vcp_confidence': mapped_item.get('vcp_confidence', 0.0),
                                    'vcp_confidence_level': vcp_confidence_level,
                                    'cup_handle_confidence': mapped_item.get('cup_handle_confidence', 0.0),
                                    'cup_handle_confidence_level': cup_handle_confidence_level
                                }
                                mapped_item = filtered_item
                                
                        elif screener_name == 'integrated_results':
                            # 패턴 인식 전 결과 (기존 로직 유지)
                            if 'rs_score' in mapped_item:
                                mapped_item['pattern_summary'] = f"RS: {mapped_item['rs_score']}"
                                
                        # For pattern results, add pattern detection summary and dimensional scores
                        if screener_name in ['image_pattern_results'] and should_include:
                            pattern_info = []
                            if mapped_item.get('vcp_detected'):
                                vcp_conf = mapped_item.get('vcp_confidence', 0)
                                pattern_info.append(f"VCP({vcp_conf:.2f})")
                                
                            if mapped_item.get('cup_handle_detected'):
                                cup_conf = mapped_item.get('cup_handle_confidence', 0)
                                pattern_info.append(f"C&H({cup_conf:.2f})")
                                    
                            mapped_item['pattern_summary'] = ', '.join(pattern_info) if pattern_info else 'No patterns'
                            
                        if should_include:
                            mapped_data.append(mapped_item)
                    
                    mtime = os.path.getmtime(csv_file)
                    return jsonify({
                        'success': True,
                        'data': mapped_data,
                        'total_count': len(mapped_data),
                        'last_updated': datetime.fromtimestamp(mtime).isoformat()
                    })
                except Exception as e:
                    return jsonify({'success': False, 'error': f'Error reading CSV file: {str(e)}'}), 500
        
        # Legacy handling for pattern_detection_results
        if not os.path.exists(json_file) and screener_name == 'pattern_detection_results':
            # Try multiple possible locations
            possible_paths = [
                os.path.join(MARKMINERVINI_RESULTS_DIR, 'pattern_detection_results.json'),
                os.path.join(RESULTS_DIR, 'screeners', 'markminervini', 'pattern_detection_results.json'),
                os.path.join(RESULTS_DIR, 'results2', 'pattern_analysis_results.json')
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    json_file = path
                    break
        
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    content = f.read().strip()
                    if not content:  # 빈 파일 처리
                        data = []
                    else:
                        data = json.loads(content)
                
                # Handle nested JSON structure for pattern_detection_results
                if screener_name == 'pattern_detection_results' and isinstance(data, dict) and 'results' in data:
                    data = data['results']  # Extract the results array from nested structure
                
                # NaN 값 처리
                data = sanitize_json_data(data)
                mtime = os.path.getmtime(json_file)
                return jsonify({
                    'success': True,
                    'data': data,
                    'total_count': len(data) if isinstance(data, list) else 0,
                    'last_updated': datetime.fromtimestamp(mtime).isoformat()
                })
            except json.JSONDecodeError as e:
                return jsonify({'success': False, 'error': f'Invalid JSON format in {screener_name}: {str(e)}'}), 500
            except Exception as e:
                return jsonify({'success': False, 'error': f'Error reading file {screener_name}: {str(e)}'}), 500
        else:
            return jsonify({'success': False, 'error': f'File not found: {screener_name}. Searched paths: {[os.path.join(MARKMINERVINI_RESULTS_DIR, f"{screener_name}.json"), os.path.join(RESULTS_DIR, f"{screener_name}.json")]}'}), 404
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# 쿨라매기 매매법 관련 API 엔드포인트 추가
@app.route('/api/qullamaggie/description', methods=['GET'])
def get_qullamaggie_description():
    """쿨라매기 매매법 설명 텍스트 반환"""
    try:
        # 프로젝트 루트 디렉토리 기준으로 절대경로 설정
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        md_path = os.path.join(project_root, 'screeners', 'qullamaggie', 'pattern.md')
        if os.path.exists(md_path):
            with open(md_path, 'r', encoding='utf-8') as f:
                text = f.read()
            return jsonify({'success': True, 'data': text})
        return jsonify({'success': False, 'message': 'Description not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/qullamaggie/breakout', methods=['GET'])
def get_qullamaggie_breakout_results():
    """쿨라매기 브레이크아웃 셋업 결과 반환"""
    try:
        json_file = os.path.join(RESULTS_DIR, 'screeners', 'qullamaggie', 'breakout_results.json')
        if os.path.exists(json_file):
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify({
                'success': True,
                'data': data,
                'total_count': len(data)
            })
        else:
            return jsonify({'success': False, 'message': 'Breakout setup data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/qullamaggie/episode-pivot', methods=['GET'])
def get_qullamaggie_episode_pivot_results():
    """쿨라매기 에피소드 피벗 셋업 결과 반환"""
    try:
        qullamaggie_dir = os.path.join(RESULTS_DIR, 'screeners', 'qullamaggie')
        df, _ = _load_latest_json(qullamaggie_dir, 'episode_pivot_results')
        
        if df is not None:
            # DataFrame을 JSON 직렬화 가능한 형태로 변환
            data = sanitize_json_data(df.to_dict('records'))
            return jsonify({
                'success': True,
                'data': data,
                'total_count': len(data)
            })
        else:
            return jsonify({'success': False, 'message': 'Episode pivot setup data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/qullamaggie/parabolic-short', methods=['GET'])
def get_qullamaggie_parabolic_short_results():
    """쿨라매기 파라볼릭 숏 셋업 결과 반환"""
    try:
        qullamaggie_dir = os.path.join(RESULTS_DIR, 'screeners', 'qullamaggie')
        df, _ = _load_latest_json(qullamaggie_dir, 'parabolic_short_results')
        
        if df is not None:
            # DataFrame을 JSON 직렬화 가능한 형태로 변환
            data = sanitize_json_data(df.to_dict('records'))
            return jsonify({
                'success': True,
                'data': data,
                'total_count': len(data)
            })
        else:
            return jsonify({'success': False, 'message': 'Parabolic short setup data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/qullamaggie/buy-signals', methods=['GET'])
def get_qullamaggie_buy_signals():
    """쿨라매기 매수 시그널 결과 반환"""
    try:
        json_file = os.path.join(RESULTS_DIR, 'screeners', 'qullamaggie', 'buy', 'qullamaggie_buy_signals.json')
        if os.path.exists(json_file):
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify({
                'success': True,
                'data': data,
                'total_count': len(data)
            })
        else:
            return jsonify({'success': False, 'message': 'Buy signals data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/qullamaggie/sell-signals', methods=['GET'])
def get_qullamaggie_sell_signals():
    """쿨라매기 매도 시그널 결과 반환"""
    try:
        json_file = os.path.join(RESULTS_DIR, 'screeners', 'qullamaggie', 'sell', 'qullamaggie_sell_signals.json')
        if os.path.exists(json_file):
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify({
                'success': True,
                'data': data,
                'total_count': len(data)
            })
        else:
            return jsonify({'success': False, 'message': 'Sell signals data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/qullamaggie/run', methods=['POST'])
def run_qullamaggie_screening():
    """쿨라매기 매매법 스크리닝 실행"""
    try:
        from screeners.qullamaggie import run_qullamaggie_strategy

        data = request.get_json()
        mode = data.get('mode', 'all')  # all, breakout, episode_pivot, parabolic_short

        if mode == 'all':
            setups = ['breakout', 'episode_pivot', 'parabolic_short']
        else:
            setups = [mode]

        success = run_qullamaggie_strategy(setups)

        return jsonify({
            'success': success,
            'message': 'Qullamaggie screening completed' if success else 'Qullamaggie screening failed'
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/recent-signals', methods=['GET'])
def get_recent_signals():
    """최근 시그널 포착된 종목들 반환 (스크리너별 독립적)"""
    try:
        from datetime import datetime, timedelta
        
        # 최근 며칠 이내 시그널을 확인할지 (기본 5일)
        days = int(request.args.get('days', 5))
        cutoff_date = datetime.now() - timedelta(days=days)
        
        recent_signals = []
        
        # 각 스크리너별로 최근 시그널 확인
        screeners = {
            'momentum_signals': MOMENTUM_SIGNALS_RESULTS_DIR,
            'leader_stock': LEADER_STOCK_RESULTS_DIR,
            'us_gainer': os.path.join(RESULTS_DIR, 'screeners', 'us_gainer'),
            'us_setup': os.path.join(RESULTS_DIR, 'screeners', 'us_setup'),
            'markminervini': MARKMINERVINI_RESULTS_DIR,
            'volatility_skew': os.path.join(RESULTS_DIR, 'screeners', 'option_volatility')
        }
        
        for screener_name, screener_dir in screeners.items():
            try:
                print(f"Processing screener: {screener_name}, dir: {screener_dir}")
                # 해당 스크리너의 최신 결과 파일 찾기
                if os.path.exists(screener_dir):
                    json_files = glob.glob(os.path.join(screener_dir, '*.json'))
                    if json_files:
                        # 개선된 파일 선택 로직: 타임스탬프 우선, 파일 시스템 시간 보조
                        def _get_file_timestamp_priority(file_path: str) -> tuple[int, datetime]:
                            """파일 우선순위 계산 (파일명 타임스탬프 > 파일 시스템 시간)"""
                            filename = os.path.basename(file_path)
                            
                            # 파일명에서 타임스탬프 추출 시도
                            import re
                            # _YYYYMMDD 형식 매칭
                            match = re.search(r'_(\d{8})(?:\.json)?$', filename)
                            if match:
                                try:
                                    timestamp = datetime.strptime(match.group(1), '%Y%m%d')
                                    return (1, timestamp)  # 높은 우선순위
                                except ValueError:
                                    pass
                            
                            # _YYYY-MM-DD 형식 매칭
                            match = re.search(r'_(\d{4}-\d{2}-\d{2})(?:\.json)?$', filename)
                            if match:
                                try:
                                    timestamp = datetime.strptime(match.group(1), '%Y-%m-%d')
                                    return (1, timestamp)  # 높은 우선순위
                                except ValueError:
                                    pass
                            
                            # 파일 시스템 수정 시간 사용
                            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                            return (0, file_mtime)  # 낮은 우선순위
                        
                        # 우선순위 기준으로 최신 파일 선택
                        latest_file = max(json_files, key=_get_file_timestamp_priority)
                        
                        # 파일의 실제 타임스탬프 또는 파일 시스템 시간 가져오기
                        priority, file_timestamp = _get_file_timestamp_priority(latest_file)
                        file_mtime = file_timestamp
                        
                        # 최근 시그널인지 확인
                        if file_mtime >= cutoff_date:
                            # 파일 크기 확인 (빈 파일 건너뛰기)
                            if os.path.getsize(latest_file) == 0:
                                print(f"Skipping empty file: {latest_file}")
                                continue
                            
                            try:
                                with open(latest_file, 'r', encoding='utf-8') as f:
                                    data = json.load(f)
                            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                                print(f"Error reading JSON file {latest_file}: {e}")
                                continue
                            
                            # 데이터가 리스트인 경우
                            if isinstance(data, list) and data:
                                for item in data[:10]:  # 최대 10개만
                                    # 심볼 정보 추출
                                    symbol = item.get('symbol', item.get('ticker', item.get('종목명', 'N/A')))
                                    
                                    # 현재가 정보 추출 - 더 많은 필드명 확인
                                    price = item.get('price', item.get('close', item.get('현재가', item.get('Close', item.get('current_price', 'N/A')))))
                                    
                                    # 변화율 추출 - 더 많은 필드명 확인
                                    change_pct = item.get('change_pct', item.get('변화율', item.get('pct_change', item.get('Change%', item.get('change_percent', 'N/A')))))
                                    
                                    # RS 점수 추출 - 스크리너별 특별 처리
                                    if screener_name == 'momentum_signals':
                                        # 모멘텀 시그널의 경우 rs_score 필드 직접 확인
                                        rs_score = item.get('rs_score', 'N/A')
                                        if rs_score != 'N/A' and isinstance(rs_score, (int, float)):
                                            rs_score = round(float(rs_score), 1)
                                    else:
                                        # 다른 스크리너들의 경우 기존 로직 사용
                                        rs_score = item.get('rs_score', item.get('RS점수', item.get('relative_strength', item.get('RS_Score', item.get('rs_percentile', 'N/A')))))
                                    
                                    # 시그널 발생일 추출
                                    signal_date = item.get('signal_date', item.get('date', item.get('detection_date', item.get('processing_date', file_mtime.strftime('%Y-%m-%d')))))
                                    if signal_date and signal_date != 'N/A':
                                        # 날짜 형식 정규화
                                        try:
                                            from datetime import datetime
                                            if isinstance(signal_date, str):
                                                # 다양한 날짜 형식 처리
                                                if signal_date.startswith('1970') or signal_date == '1970-01-01':
                                                    signal_date = file_mtime.strftime('%Y-%m-%d')
                                                else:
                                                    # 날짜 형식 검증
                                                    datetime.strptime(signal_date, '%Y-%m-%d')
                                            elif isinstance(signal_date, (int, float)):
                                                # timestamp인 경우
                                                if signal_date < 86400:  # 1970년 1월 2일 이전
                                                    signal_date = file_mtime.strftime('%Y-%m-%d')
                                                else:
                                                    signal_date = datetime.fromtimestamp(signal_date).strftime('%Y-%m-%d')
                                        except:
                                            signal_date = file_mtime.strftime('%Y-%m-%d')
                                    else:
                                        signal_date = file_mtime.strftime('%Y-%m-%d')
                                    
                                    # 실시간 가격 정보 가져오기 (yfinance 사용)
                                    if price == 'N/A' or change_pct == 'N/A':
                                        try:
                                            import yfinance as yf
                                            ticker = yf.Ticker(symbol)
                                            hist = ticker.history(period='2d')
                                            if not hist.empty and len(hist) >= 2:
                                                current_price = hist['Close'].iloc[-1]
                                                prev_price = hist['Close'].iloc[-2]
                                                if price == 'N/A':
                                                    price = round(float(current_price), 2)
                                                if change_pct == 'N/A':
                                                    change_pct = round(((current_price - prev_price) / prev_price) * 100, 2)
                                        except Exception:
                                            pass
                                    
                                    # 스크리너별 특별 처리
                                    if screener_name == 'markminervini':
                                        # markminervini에서 추가 필드 확인
                                        if price == 'N/A':
                                            price = item.get('Current_Price', 'N/A')
                                        if change_pct == 'N/A':
                                            change_pct = 'N/A'  # markminervini에는 변화율 정보가 없음
                                    elif screener_name in ['us_gainer', 'us_setup']:
                                        # us_gainer, us_setup에서 성과 기반 RS 점수 계산
                                        if rs_score == 'N/A':
                                            # 1개월 성과를 기반으로 간단한 RS 점수 계산
                                            perf_1m = item.get('perf_1m_pct', item.get('change_pct', 0))
                                            if isinstance(perf_1m, (int, float)) and perf_1m > 0:
                                                # 성과를 0-100 범위로 매핑 (간단한 방식)
                                                rs_score = min(100, max(0, 50 + perf_1m * 2))
                                                rs_score = round(rs_score, 1)
                                            else:
                                                rs_score = 'N/A'
                                    
                                    signal_info = {
                                        'screener': screener_name,
                                        'symbol': symbol,
                                        'signal_date': signal_date,
                                        'price': price if price != 'N/A' else 'N/A',
                                        'change_pct': change_pct if change_pct != 'N/A' else 'N/A',
                                        'rs_score': rs_score if rs_score != 'N/A' else 'N/A',
                                        'company_name': item.get('company_name', item.get('Company_Name', item.get('종목명', 'N/A'))),
                                        # Pattern detection fields for compatibility
                                        'vcp_detected': item.get('vcp_detected'),
                                        'VCP_Pattern': item.get('vcp_detected', item.get('VCP_Pattern')),
                                        'cup_handle_detected': item.get('cup_handle_detected'),
                                        'Cup_Handle_Pattern': item.get('cup_handle_detected', item.get('Cup_Handle_Pattern'))
                                    }
                                    recent_signals.append(signal_info)
            except Exception as e:
                print(f"Error processing {screener_name}: {e}")
                continue
        
        # 시그널 발생일 기준으로 정렬 (최신순)
        recent_signals.sort(key=lambda x: x['signal_date'], reverse=True)
        
        # NaN 값 처리
        sanitized_data = sanitize_json_data(recent_signals)
        
        return jsonify({
            'success': True,
            'data': sanitized_data,
            'total_count': len(sanitized_data),
            'days_filter': days
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/top-stocks', methods=['GET'])
def get_top_stocks():
    """매수 랭킹 상위 10개 종목 반환 (markminervini 패턴 감지된 결과만 사용)"""
    try:
        # TOPSIS 기반 랭킹 파일 우선 사용
        ranking_file = os.path.join(RESULTS_DIR, 'ranking_results.csv')
        
        if not os.path.exists(ranking_file):
            # 대안으로 ranking 디렉토리의 파일 확인
            ranking_file = os.path.join(RESULTS_DIR, 'ranking', 'ranking_results.csv')
        
        if os.path.exists(ranking_file):
            df = pd.read_csv(ranking_file)
            # NaN 값을 None으로 변환
            df = df.where(pd.notnull(df), None)
            
            # 상위 10개 선택
            top_10 = df.head(10)
            
            # 결과 포맷팅 - 핵심 데이터만 포함
            top_stocks = []
            for _, row in top_10.iterrows():
                # RS 점수는 이미 Mark Minervini 방식으로 0-100 범위로 계산됨
                rs_score = row.get('relative_strength', 0)
                if not isinstance(rs_score, (int, float)) or rs_score < 0:
                    rs_score = 0
                
                stock_info = {
                    'symbol': row.get('symbol', 'N/A'),
                    'rank': int(row.get('rank', 0)),
                    'topsis_score': float(row.get('score', 0)),
                    'rs_score': round(float(rs_score), 1),
                    'price_momentum_20d': float(row.get('price_momentum_20d', 0)),
                    # Pattern detection fields for compatibility
                    'vcp_detected': row.get('vcp_detected'),
                    'VCP_Pattern': row.get('vcp_detected', row.get('VCP_Pattern')),
                    'cup_handle_detected': row.get('cup_handle_detected'),
                    'Cup_Handle_Pattern': row.get('cup_handle_detected', row.get('Cup_Handle_Pattern'))
                }
                top_stocks.append(stock_info)
            
            mtime = os.path.getmtime(ranking_file)
            
            # NaN 값 처리
            sanitized_data = sanitize_json_data(top_stocks)
            
            return jsonify({
                'success': True,
                'data': sanitized_data,
                'total_count': len(sanitized_data),
                'last_updated': datetime.fromtimestamp(mtime).isoformat()
            })
        
        # 패턴 감지 결과가 없으면 기존 랭킹 파일 사용 (fallback)
        ranking_file = os.path.join(RESULTS_DIR, 'ranking', 'ranking_results.csv')
        
        if not os.path.exists(ranking_file):
            # 대안으로 루트 디렉토리의 ranking_results.csv 확인
            ranking_file = os.path.join(RESULTS_DIR, 'ranking_results.csv')
        
        if os.path.exists(ranking_file):
            df = pd.read_csv(ranking_file)
            # NaN 값을 None으로 변환
            df = df.where(pd.notnull(df), None)
            
            # 상위 10개 선택
            top_10 = df.head(10)
            
            # 결과 포맷팅 - 핵심 데이터만 포함
            top_stocks = []
            for _, row in top_10.iterrows():
                # RS 점수는 이미 Mark Minervini 방식으로 0-100 범위로 계산됨
                rs_score = row.get('relative_strength', 0)
                if not isinstance(rs_score, (int, float)) or rs_score < 0:
                    rs_score = 0
                
                stock_info = {
                    'symbol': row.get('symbol', 'N/A'),
                    'rank': int(row.get('rank', 0)),
                    'topsis_score': float(row.get('score', row.get('topsis_score', 0))),
                    'rs_score': round(float(rs_score), 1),
                    'price_momentum_20d': float(row.get('price_momentum_20d', 0)),
                    # Pattern detection fields for compatibility
                    'vcp_detected': row.get('vcp_detected'),
                    'VCP_Pattern': row.get('vcp_detected', row.get('VCP_Pattern')),
                    'cup_handle_detected': row.get('cup_handle_detected'),
                    'Cup_Handle_Pattern': row.get('cup_handle_detected', row.get('Cup_Handle_Pattern'))
                }
                top_stocks.append(stock_info)
            
            mtime = os.path.getmtime(ranking_file)
            
            # NaN 값 처리
            sanitized_data = sanitize_json_data(top_stocks)
            
            return jsonify({
                'success': True,
                'data': sanitized_data,
                'total_count': len(sanitized_data),
                'last_updated': datetime.fromtimestamp(mtime).isoformat()
            })
        else:
            return jsonify({'success': False, 'message': 'Ranking data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/dashboard-summary', methods=['GET'])
def get_dashboard_summary():
    """대시보드 요약 정보 반환"""
    try:
        # 최근 시그널 정보 수집
        recent_signals_data = {
            'total_signals': 0,
            'screeners_active': 0,
            'last_updated': datetime.now().isoformat()
        }
        
        # 각 스크리너에서 데이터 개수 확인
        screener_files = {
            'technical': os.path.join(MARKMINERVINI_RESULTS_DIR, 'us_with_rs.json'),
            'financial': os.path.join(MARKMINERVINI_RESULTS_DIR, 'advanced_financial_results.json'),
            'integrated': os.path.join(MARKMINERVINI_RESULTS_DIR, 'integrated_results.json')
        }
        
        total_signals = 0
        active_screeners = 0
        latest_update = None
        
        for name, file_path in screener_files.items():
            if os.path.exists(file_path):
                try:
                    df = pd.read_json(file_path)
                    # NaN 값을 None으로 변환
                    df = df.where(pd.notnull(df), None)
                    total_signals += len(df)
                    active_screeners += 1
                    
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if latest_update is None or file_mtime > latest_update:
                        latest_update = file_mtime
                except Exception:
                    pass
        
        recent_signals_data['total_signals'] = total_signals
        recent_signals_data['screeners_active'] = active_screeners
        if latest_update:
            recent_signals_data['last_updated'] = latest_update.isoformat()
        
        # Top stocks 정보
        top_stocks_data = {
            'available': False,
            'top_score': 0.0,
            'last_updated': datetime.now().isoformat()
        }
        
        # 랭킹 파일 확인
        ranking_files = [
            os.path.join(RESULTS_DIR, 'ranking', 'ranking_results.csv'),
            os.path.join(RESULTS_DIR, 'ranking_results.csv')
        ]
        
        for ranking_file in ranking_files:
            if os.path.exists(ranking_file):
                try:
                    df = pd.read_csv(ranking_file)
                    # NaN 값을 None으로 변환
                    df = df.where(pd.notnull(df), None)
                    if not df.empty:
                        # topsis_score 또는 score 컬럼 확인
                        score_column = None
                        if 'topsis_score' in df.columns:
                            score_column = 'topsis_score'
                        elif 'score' in df.columns:
                            score_column = 'score'
                        
                        if score_column:
                            top_stocks_data['available'] = True
                            top_stocks_data['top_score'] = float(df[score_column].max())
                            top_stocks_data['last_updated'] = datetime.fromtimestamp(os.path.getmtime(ranking_file)).isoformat()
                            break
                except Exception:
                    pass
        
        # 마켓 레짐 정보
        market_regime_data = {
            'current_regime': 'Unknown',
            'confidence': 0.0,
            'last_updated': datetime.now().isoformat()
        }
        
        market_regime_file = os.path.join(MARKET_REGIME_DIR, 'latest_market_regime.json')
        if os.path.exists(market_regime_file):
            try:
                with open(market_regime_file, 'r', encoding='utf-8') as f:
                    regime_data = json.load(f)
                    market_regime_data['current_regime'] = regime_data.get('regime', 'Unknown')
                    # confidence 필드가 없으면 score를 100으로 나누어 사용
                    confidence = regime_data.get('confidence')
                    if confidence is None:
                        score = regime_data.get('score', 0)
                        confidence = score / 100.0 if score > 0 else 0.0
                    market_regime_data['confidence'] = float(confidence)
                    market_regime_data['last_updated'] = datetime.fromtimestamp(os.path.getmtime(market_regime_file)).isoformat()
            except Exception:
                pass
        
        # 스크리너 상태 정보
        screeners_status = {}
        
        for name, file_path in screener_files.items():
            if os.path.exists(file_path):
                try:
                    df = pd.read_json(file_path)
                    # NaN 값을 None으로 변환
                    df = df.where(pd.notnull(df), None)
                    screeners_status[name] = {
                        'available': True,
                        'count': len(df),
                        'last_updated': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                    }
                except Exception:
                    screeners_status[name] = {
                        'available': False,
                        'count': 0,
                        'last_updated': datetime.now().isoformat()
                    }
            else:
                screeners_status[name] = {
                    'available': False,
                    'count': 0,
                    'last_updated': datetime.now().isoformat()
                }
        
        # 프론트엔드에서 기대하는 구조로 반환
        return jsonify({
            'success': True,
            'data': {
                'recent_signals': recent_signals_data,
                'top_stocks': top_stocks_data,
                'market_regime': market_regime_data,
                'screeners_status': screeners_status
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

if __name__ == '__main__':
    # 기본 포트를 프론트엔드 기본값(5000)과 맞춰 연결 오류를 방지한다
    port = int(os.getenv('BACKEND_PORT', 5000))
    debug = os.getenv('FLASK_ENV', 'development') == 'development'
    app.run(debug=debug, host='0.0.0.0', port=port)
