"""Academic pattern detection wrapper.

This script uses advanced algorithms based on `new VCP & CUPhandle.md`
for detecting Volatility Contraction and Cup & Handle patterns.
It simply exposes a CLI around the existing implementation
in `screeners.markminervini.pattern_detection` for backward compatibility.
"""

from screeners.markminervini.pattern_detection import (
    run_pattern_detection_on_financial_results,
)


def main() -> None:
    """Run pattern detection using academic rules."""
    print("🔬 패턴 감지 시작...")
    results = run_pattern_detection_on_financial_results()
    if results is not None and not results.empty:
        print(f"✅ 패턴 분석 완료: {len(results)}개 종목 감지")
    else:
        print("⚠️ 패턴을 만족하는 종목이 없습니다.")


if __name__ == "__main__":
    main()

