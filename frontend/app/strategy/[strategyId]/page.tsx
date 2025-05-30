// 이 파일의 경로는 frontend/app/strategy/[strategyId]/page.tsx 입니다.

interface StrategyPageProps {
  params: {
    strategyId: string;
  };
}

export default function StrategyPage({ params }: StrategyPageProps) {
  return (
    <div>
      <h1 className="text-2xl font-bold">Strategy Details</h1>
      <p>Displaying data for strategy: {params.strategyId}</p>
      {/* 여기에 해당 전략의 JSON 데이터를 불러와 표시하는 로직을 추가합니다. */}
    </div>
  );
}