import pandas as pd
from scipy.stats import percentileofscore

__all__ = ["calculate_rs_score_enhanced", "calculate_rs_score"]


def calculate_rs_score_enhanced(df: pd.DataFrame, price_col: str = "close", benchmark_symbol: str = "SPY") -> pd.Series:
    """Fred6724의 TradingView 기반 RS Rating 알고리즘을 구현한 고도화된 RS 점수 계산
    
    메모리 경합 문제 해결을 위한 개선사항:
    - 청크 단위 처리로 메모리 사용량 제한
    - 가비지 컬렉션 강제 실행
    - 메모리 효율적인 데이터 처리
    """
    import numpy as np
    import gc
    
    try:
            if not isinstance(df.index, pd.MultiIndex):
                date_col = next((col for col in ["date", "time"] if col in df.columns), None)
                symbol_col = next((col for col in ["symbol", "pair"] if col in df.columns), None)
                if date_col and symbol_col:
                    if not pd.api.types.is_datetime64_dtype(df[date_col]):
                        df[date_col] = pd.to_datetime(df[date_col], utc=True)
                    df = df.set_index([date_col, symbol_col])
                else:
                    print("❌ 날짜/심볼 컬럼을 찾을 수 없습니다.")
                    return pd.Series(dtype=float)

            if df.index.nlevels != 2:
                print(f"⚠️ 인덱스 레벨이 2가 아닙니다: {df.index.nlevels}")
                return pd.Series(dtype=float)

            try:
                benchmark_data = df.xs(benchmark_symbol, level=1)[price_col]
                if len(benchmark_data) < 252:
                    print(f"⚠️ {benchmark_symbol} 벤치마크 데이터가 부족합니다 (필요: 252일, 현재: {len(benchmark_data)}일)")
                    return pd.Series(dtype=float)
                    
                # 벤치마크 데이터를 미리 계산하여 메모리 효율성 향상
                bench_252 = benchmark_data.tail(252).values
                if len(bench_252) < 252:
                    print(f"⚠️ 벤치마크 데이터 길이 부족: {len(bench_252)}")
                    return pd.Series(dtype=float)
                    
                # 벤치마크 점수 미리 계산
                b3 = (bench_252[-1] - bench_252[-63]) / bench_252[-63] * 100
                b6 = (bench_252[-1] - bench_252[-126]) / bench_252[-126] * 100
                b9 = (bench_252[-1] - bench_252[-189]) / bench_252[-189] * 100
                b12 = (bench_252[-1] - bench_252[-252]) / bench_252[-252] * 100
                bench_score = 0.4 * b3 + 0.2 * b6 + 0.2 * b9 + 0.2 * b12
                
            except KeyError:
                print(f"⚠️ {benchmark_symbol} 벤치마크 데이터를 찾을 수 없습니다.")
                return pd.Series(dtype=float)

            # 심볼 리스트를 청크로 분할하여 메모리 사용량 제한
            symbols = [s for s in df.index.get_level_values(1).unique() if s != benchmark_symbol]
            chunk_size = min(100, max(10, len(symbols) // 4))  # 적응적 청크 크기
            symbol_chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
            
            rs_scores = {}
            
            for chunk_idx, symbol_chunk in enumerate(symbol_chunks):
                print(f"📊 RS 점수 계산 진행: {chunk_idx + 1}/{len(symbol_chunks)} 청크 ({len(symbol_chunk)}개 종목)")
                
                chunk_scores = {}
                for symbol in symbol_chunk:
                    try:
                        symbol_data = df.xs(symbol, level=1)[price_col]
                        if len(symbol_data) < 252:
                            continue
                            
                        close = symbol_data.tail(252).values
                        if len(close) < 252:
                            continue
                            
                        # 개별 종목 점수 계산
                        p3 = (close[-1] - close[-63]) / close[-63] * 100
                        p6 = (close[-1] - close[-126]) / close[-126] * 100
                        p9 = (close[-1] - close[-189]) / close[-189] * 100
                        p12 = (close[-1] - close[-252]) / close[-252] * 100
                        stock_score = 0.4 * p3 + 0.2 * p6 + 0.2 * p9 + 0.2 * p12
                        
                        if bench_score != 0:
                            rs_score = stock_score / bench_score * 100
                            chunk_scores[str(symbol)] = rs_score
                            
                    except Exception as e:
                        continue
                
                # 청크 결과를 전체 결과에 병합
                rs_scores.update(chunk_scores)
                
                # 청크 처리 후 가비지 컬렉션 실행
                if chunk_idx % 2 == 1:  # 2개 청크마다 GC 실행
                    gc.collect()

            if not rs_scores:
                print("⚠️ RS Score를 계산할 수 있는 종목이 없습니다.")
                return pd.Series(dtype=float)

            # 백분위 계산을 메모리 효율적으로 수행
            rs_score_values = list(rs_scores.values())
            print(f"✅ RS 점수 계산 완료: {len(rs_score_values)}개 종목")
            
            # 메모리 사용량 최적화를 위해 배치로 백분위 계산
            rs_ratings = {}
            for symbol, score in rs_scores.items():
                rs_ratings[symbol] = round(percentileofscore(rs_score_values, score, kind="rank"), 2)
            
            # 최종 가비지 컬렉션
            gc.collect()
            
            return pd.Series(rs_ratings)
            
    except Exception as e:
        print(f"❌ 고도화된 RS Score 계산 오류: {e}")
        gc.collect()  # 오류 발생 시에도 메모리 정리
        return pd.Series(dtype=float)


def calculate_rs_score(df: pd.DataFrame, price_col: str = "close", window: int = 126, use_enhanced: bool = True) -> pd.Series:
    """RS 점수 계산 함수. 기본적으로 고도화 버전을 사용한다.
    
    메모리 경합 문제 해결을 위한 개선사항:
    - 청크 단위 처리로 메모리 사용량 제한
    - 가비지 컬렉션 강제 실행
    - 스레드 안전성 보장
    """
    if use_enhanced:
        return calculate_rs_score_enhanced(df, price_col)

    import gc
    
    try:
            if not isinstance(df.index, pd.MultiIndex):
                date_col = next((col for col in ["date", "time"] if col in df.columns), None)
                symbol_col = next((col for col in ["symbol", "pair"] if col in df.columns), None)
                if date_col and symbol_col:
                    if not pd.api.types.is_datetime64_dtype(df[date_col]):
                        df[date_col] = pd.to_datetime(df[date_col], utc=True)
                    df = df.set_index([date_col, symbol_col])
                else:
                    print("❌ 날짜/심볼 컬럼을 찾을 수 없습니다.")
                    return pd.Series(dtype=float)

            if df.index.nlevels != 2:
                print(f"⚠️ 인덱스 레벨이 2가 아닙니다: {df.index.nlevels}")
                return pd.Series(dtype=float)

            grouped = df.groupby(level=1)[price_col]
            symbol_counts = grouped.count()
            valid_symbols = symbol_counts[symbol_counts >= window].index
            if len(valid_symbols) == 0:
                print("⚠️ 충분한 데이터가 있는 심볼이 없습니다.")
                return pd.Series(dtype=float)

            # 심볼을 청크로 분할하여 메모리 사용량 제한
            valid_symbols_list = list(valid_symbols)
            chunk_size = min(50, max(10, len(valid_symbols_list) // 4))
            symbol_chunks = [valid_symbols_list[i:i + chunk_size] for i in range(0, len(valid_symbols_list), chunk_size)]
            
            returns = {}
            
            for chunk_idx, symbol_chunk in enumerate(symbol_chunks):
                print(f"📊 기본 RS 점수 계산 진행: {chunk_idx + 1}/{len(symbol_chunks)} 청크 ({len(symbol_chunk)}개 종목)")
                
                chunk_returns = {}
                for symbol in symbol_chunk:
                    try:
                        symbol_data = df.xs(symbol, level=1)[price_col]
                        if len(symbol_data) >= window:
                            recent_data = symbol_data.iloc[-window:]
                            first_price = recent_data.iloc[0]
                            last_price = recent_data.iloc[-1]
                            if first_price > 0:
                                chunk_returns[str(symbol)] = (last_price / first_price) - 1
                    except Exception:
                        continue
                
                # 청크 결과를 전체 결과에 병합
                returns.update(chunk_returns)
                
                # 청크 처리 후 가비지 컬렉션 실행
                if chunk_idx % 2 == 1:  # 2개 청크마다 GC 실행
                    gc.collect()

            returns_series = pd.Series(returns)
            if len(returns_series) > 0:
                print(f"✅ 기본 RS 점수 계산 완료: {len(returns_series)}개 종목")
                
                # 메모리 효율적인 랭킹 계산
                from scipy.stats import rankdata
                ranks = rankdata(returns_series.values)
                rs_scores = (ranks / len(ranks)) * 100
                
                # 최종 가비지 컬렉션
                gc.collect()
                
                return pd.Series(rs_scores, index=returns_series.index)
            
            return pd.Series(dtype=float)
            
    except Exception as e:
        print(f"❌ RS Score 계산 오류: {e}")
        gc.collect()  # 오류 발생 시에도 메모리 정리
        return pd.Series(dtype=float)
