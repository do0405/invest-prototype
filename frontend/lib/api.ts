const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:5000';

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  message?: string;
  total_count?: number;
}

export interface ScreeningData {
  symbol: string;
  [key: string]: any;
}

export interface PortfolioItem {
  종목명?: string;
  symbol?: string;
  비중?: number;
  weight?: number;
  매수가?: number;
  entry_price?: number;
  [key: string]: any;
}

export interface SummaryData {
  technical_screening?: {
    count: number;
    top_5: ScreeningData[];
  };
  financial_screening?: {
    count: number;
    top_5: ScreeningData[];
  };
  integrated_screening?: {
    count: number;
    top_5: ScreeningData[];
  };
  strategies: Record<string, {
    count: number;
    active_positions: number;
  }>;
}

export class ApiClient {
  private baseUrl: string;

  constructor(baseUrl: string = API_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  private async request<T>(endpoint: string): Promise<ApiResponse<T>> {
    try {
      const response = await fetch(`${this.baseUrl}${endpoint}`);
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const data = await response.json();
      return data;
    } catch (error) {
      console.error('API request failed:', error);
      return {
        success: false,
        message: error instanceof Error ? error.message : 'Unknown error occurred'
      };
    }
  }

  // 기술적 스크리닝 결과 (백엔드 엔드포인트에 맞춤)
  async getTechnicalResults() {
    return this.request<ScreeningData[]>('/api/screening-results');
  }

  // 재무제표 스크리닝 결과
  async getFinancialResults() {
    return this.request<ScreeningData[]>('/api/financial-results');
  }

  // 통합 스크리닝 결과
  async getIntegratedResults() {
    return this.request<ScreeningData[]>('/api/integrated-results');
  }

  // 전략별 포트폴리오
  async getPortfolioByStrategy(strategyName: string) {
    return this.request<PortfolioItem[]>(`/api/portfolio/${strategyName}`);
  }

  async getStrategyDescription(strategyName: string) {
    return this.request<string>(`/api/strategy-description/${strategyName}`);
  }

  async getScreenerDescription(name: string) {
    return this.request<string>(`/api/screener-description/${name}`);
  }

  async getVolatilitySkewResults() {
    return this.request<ScreeningData[]>('/api/volatility-skew');
  }

  // 전체 요약 (여러 API를 조합하여 생성)
  async getSummary(): Promise<ApiResponse<SummaryData>> {
    try {
      const [technical, financial, integrated] = await Promise.all([
        this.getTechnicalResults(),
        this.getFinancialResults(),
        this.getIntegratedResults()
      ]);

      const summaryData: SummaryData = {
        strategies: {} // 실제 전략 데이터는 별도로 구현 필요
      };

      if (technical.success && technical.data) {
        summaryData.technical_screening = {
          count: technical.total_count || technical.data.length,
          top_5: technical.data.slice(0, 5)
        };
      }

      if (financial.success && financial.data) {
        summaryData.financial_screening = {
          count: financial.total_count || financial.data.length,
          top_5: financial.data.slice(0, 5)
        };
      }

      if (integrated.success && integrated.data) {
        summaryData.integrated_screening = {
          count: integrated.total_count || integrated.data.length,
          top_5: integrated.data.slice(0, 5)
        };
      }

      return {
        success: true,
        data: summaryData
      };
    } catch (error) {
      return {
        success: false,
        message: error instanceof Error ? error.message : 'Failed to fetch summary'
      };
    }
  }
}

export const apiClient = new ApiClient();