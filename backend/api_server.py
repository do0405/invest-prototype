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
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.path_utils import add_project_root

# 프로젝트 루트 디렉토리를 Python 경로에 추가
add_project_root()

from config import (
    RESULTS_DIR,
    PORTFOLIO_RESULTS_DIR,
    MARKMINERVINI_RESULTS_DIR,
    IPO_INVESTMENT_RESULTS_DIR,
    LEADER_STOCK_RESULTS_DIR,
    MOMENTUM_SIGNALS_RESULTS_DIR,
    MARKET_REGIME_DIR,
)
from typing import Optional

app = Flask(__name__)
CORS(app)  # CORS 허용

@app.route('/api/screening-results', methods=['GET'])
def get_screening_results():
    """스크리닝 결과 반환"""
    try:
        # us_with_rs.json 파일 읽기
        json_file = os.path.join(MARKMINERVINI_RESULTS_DIR, 'us_with_rs.json')
        if os.path.exists(json_file):
            df = pd.read_json(json_file)
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
    """전략별 포트폴리오 결과 반환"""
    try:
        # Check in buy directory first
        json_file = os.path.join(PORTFOLIO_RESULTS_DIR, 'buy', f'{strategy_name}_results.json')
        if not os.path.exists(json_file):
            # Check in sell directory
            json_file = os.path.join(PORTFOLIO_RESULTS_DIR, 'sell', f'{strategy_name}_results.json')
        
        if os.path.exists(json_file):
            df = pd.read_json(json_file)
            return jsonify({
                'success': True,
                'strategy': strategy_name,
                'data': df.to_dict('records'),
                'total_count': len(df)
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
        # 가장 최근 파일 찾기
        pattern = os.path.join(RESULTS_DIR, 'screeners', 'option_volatility', 'volatility_skew_screening_*.json')
        files = glob.glob(pattern)
        if files:
            latest_file = max(files, key=os.path.getctime)
            df = pd.read_json(latest_file)
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

def _load_latest_json(directory: str) -> tuple[Optional[pd.DataFrame], Optional[float]]:
    pattern = os.path.join(directory, '*.json')
    files = glob.glob(pattern)
    if not files:
        return None, None
    latest = max(files, key=os.path.getctime)
    try:
        df = pd.read_json(latest)
        return df, os.path.getmtime(latest)
    except Exception:
        return None, None


@app.route('/api/ipo-investment', methods=['GET'])
def get_ipo_investment_results():
    """Return latest IPO investment screener results."""
    try:
        df, mtime = _load_latest_json(IPO_INVESTMENT_RESULTS_DIR)
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
        df, mtime = _load_latest_json(LEADER_STOCK_RESULTS_DIR)
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
        df, mtime = _load_latest_json(MOMENTUM_SIGNALS_RESULTS_DIR)
        if df is not None:
            return jsonify({'success': True, 'data': df.to_dict('records'), 'total_count': len(df),
                            'last_updated': datetime.fromtimestamp(mtime).isoformat() if mtime else None})
        return jsonify({'success': False, 'message': 'Momentum signals data not found'}), 404
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
        
        # Special handling for pattern_detection_results
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
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            mtime = os.path.getmtime(json_file)
            return jsonify({
                'success': True,
                'data': data,
                'total_count': len(data) if isinstance(data, list) else 0,
                'last_updated': datetime.fromtimestamp(mtime).isoformat()
            })
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
        json_file = os.path.join(RESULTS_DIR, 'screeners', 'qullamaggie', 'episode_pivot_results.json')
        if os.path.exists(json_file):
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
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
        json_file = os.path.join(RESULTS_DIR, 'screeners', 'qullamaggie', 'parabolic_short_results.json')
        if os.path.exists(json_file):
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
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
                # 해당 스크리너의 최신 결과 파일 찾기
                if os.path.exists(screener_dir):
                    json_files = glob.glob(os.path.join(screener_dir, '*.json'))
                    if json_files:
                        # 가장 최근 파일 선택
                        latest_file = max(json_files, key=os.path.getmtime)
                        file_mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
                        
                        # 최근 시그널인지 확인
                        if file_mtime >= cutoff_date:
                            with open(latest_file, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                            
                            # 데이터가 리스트인 경우
                            if isinstance(data, list) and data:
                                for item in data[:10]:  # 최대 10개만
                                    signal_info = {
                                        'screener': screener_name,
                                        'symbol': item.get('symbol', item.get('ticker', item.get('종목명', 'N/A'))),
                                        'signal_date': file_mtime.strftime('%Y-%m-%d'),
                                        'price': item.get('close', item.get('현재가', 'N/A')),
                                        'change_pct': item.get('change_pct', item.get('변화율', 'N/A')),
                                        'rs_score': item.get('rs_score', item.get('RS점수', 'N/A'))
                                    }
                                    recent_signals.append(signal_info)
            except Exception as e:
                print(f"Error processing {screener_name}: {e}")
                continue
        
        # 시그널 발생일 기준으로 정렬 (최신순)
        recent_signals.sort(key=lambda x: x['signal_date'], reverse=True)
        
        return jsonify({
            'success': True,
            'data': recent_signals,
            'total_count': len(recent_signals),
            'days_filter': days
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/top-stocks', methods=['GET'])
def get_top_stocks():
    """매수 랭킹 상위 10개 종목 반환"""
    try:
        # 랭킹 결과 파일 경로
        ranking_file = os.path.join(RESULTS_DIR, 'ranking', 'ranking_results.csv')
        
        if not os.path.exists(ranking_file):
            # 대안으로 루트 디렉토리의 ranking_results.csv 확인
            ranking_file = os.path.join(RESULTS_DIR, 'ranking_results.csv')
        
        if os.path.exists(ranking_file):
            df = pd.read_csv(ranking_file)
            
            # 상위 10개 선택
            top_10 = df.head(10)
            
            # 결과 포맷팅
            top_stocks = []
            for _, row in top_10.iterrows():
                stock_info = {
                    'symbol': row.get('symbol', 'N/A'),
                    'rank': int(row.get('rank', 0)),
                    'score': float(row.get('score', 0)),
                    'price_momentum_20d': float(row.get('price_momentum_20d', 0)),
                    'rsi_14': float(row.get('rsi_14', 0)),
                    'pe_ratio': float(row.get('pe_ratio', 0)),
                    'roe': float(row.get('roe', 0)),
                    'relative_strength': float(row.get('relative_strength', 0))
                }
                top_stocks.append(stock_info)
            
            mtime = os.path.getmtime(ranking_file)
            
            return jsonify({
                'success': True,
                'data': top_stocks,
                'total_count': len(top_stocks),
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
