from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
from config import RESULTS_DIR, RESULTS_VER2_DIR
import glob

app = Flask(__name__)
CORS(app)  # CORS 허용

@app.route('/api/screening-results', methods=['GET'])
def get_screening_results():
    """스크리닝 결과 반환"""
    try:
        # us_with_rs.json 파일 읽기
        json_file = os.path.join(RESULTS_DIR, 'us_with_rs.json')
        if os.path.exists(json_file):
            df = pd.read_json(json_file)
            return jsonify({
                'success': True,
                'data': df.to_dict('records'),
                'total_count': len(df)
            })
        else:
            return jsonify({'success': False, 'message': 'Data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/financial-results', methods=['GET'])
def get_financial_results():
    """재무제표 스크리닝 결과 반환"""
    try:
        json_file = os.path.join(RESULTS_DIR, 'advanced_financial_results.json')
        if os.path.exists(json_file):
            df = pd.read_json(json_file)
            return jsonify({
                'success': True,
                'data': df.to_dict('records'),
                'total_count': len(df)
            })
        else:
            return jsonify({'success': False, 'message': 'Data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/integrated-results', methods=['GET'])
def get_integrated_results():
    """통합 스크리닝 결과 반환"""
    try:
        json_file = os.path.join(RESULTS_DIR, 'integrated_results.json')
        if os.path.exists(json_file):
            df = pd.read_json(json_file)
            return jsonify({
                'success': True,
                'data': df.to_dict('records'),
                'total_count': len(df)
            })
        else:
            return jsonify({'success': False, 'message': 'Data not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@app.route('/api/portfolio/<strategy_name>', methods=['GET'])
def get_portfolio_by_strategy(strategy_name):
    """전략별 포트폴리오 결과 반환"""
    try:
        json_file = os.path.join(RESULTS_VER2_DIR, f'{strategy_name}_results.json')
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

@app.route('/api/volatility-skew', methods=['GET'])
def get_volatility_skew_results():
    """변동성 스큐 스크리닝 결과 반환"""
    try:
        # 가장 최근 파일 찾기
        pattern = os.path.join(RESULTS_VER2_DIR, 'option_volatility', 'volatility_skew_screening_*.json')
        files = glob.glob(pattern)
        if files:
            latest_file = max(files, key=os.path.getctime)
            df = pd.read_json(latest_file)
            return jsonify({
                'success': True,
                'data': df.to_dict('records'),
                'total_count': len(df),
                'file_timestamp': os.path.basename(latest_file)
            })
        else:
            return jsonify({'success': False, 'message': 'Volatility skew data not found'}), 404
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)