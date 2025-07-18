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
        pattern = os.path.join(PORTFOLIO_RESULTS_DIR, 'option_volatility', 'volatility_skew_screening_*.json')
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
        # Check main results directory
        json_file = os.path.join(RESULTS_DIR, f'{screener_name}.json')
        
        # Backward compatibility for old pattern analysis path
        if not os.path.exists(json_file) and screener_name == 'pattern_detection_results':
            json_file = os.path.join(RESULTS_DIR, 'results2', 'pattern_analysis_results.json')
        
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
            return jsonify({'success': False, 'error': f'File not found: {screener_name}'}), 404
            
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
        json_file = os.path.join(PORTFOLIO_RESULTS_DIR, 'qullamaggie_result', 'breakout_results.json')
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
        json_file = os.path.join(PORTFOLIO_RESULTS_DIR, 'qullamaggie_result', 'episode_pivot_results.json')
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
        json_file = os.path.join(PORTFOLIO_RESULTS_DIR, 'qullamaggie_result', 'parabolic_short_results.json')
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
        json_file = os.path.join(PORTFOLIO_RESULTS_DIR, 'qullamaggie_result', 'buy', 'qullamaggie_buy_signals.json')
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
        json_file = os.path.join(PORTFOLIO_RESULTS_DIR, 'qullamaggie_result', 'sell', 'qullamaggie_sell_signals.json')
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

if __name__ == '__main__':
    # 기본 포트를 프론트엔드 기본값(5000)과 맞춰 연결 오류를 방지한다
    port = int(os.getenv('BACKEND_PORT', 5000))
    debug = os.getenv('FLASK_ENV', 'development') == 'development'    app.run(debug=debug, host='0.0.0.0', port=port)
