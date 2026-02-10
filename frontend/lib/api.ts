const API_BASE_URL = '';

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  message?: string;
  total_count?: number;
  last_updated?: string;
}

export interface ScreeningData {
  symbol: string;
  // Common fields
  rs_score?: number;
  signal_date?: string;
  met_count?: number;
  price?: number;
  change_pct?: number;
  
  // Pattern detection fields (both naming conventions supported)
  vcp_detected?: boolean;
  VCP_Pattern?: boolean;
  cup_handle_detected?: boolean;
  Cup_Handle_Pattern?: boolean;
  vcp_confidence?: number;
  cup_handle_confidence?: number;
  vcp_confidence_level?: string;
  cup_handle_confidence_level?: string;
  
  // Financial fields
  fin_met_count?: number;
  total_met_count?: number;
  
  // Technical fields
  volume?: number;
  market_cap?: number;
  adr_percent?: number;
  perf_1w_pct?: number;
  perf_1m_pct?: number;
  
  // Flexible for additional fields
  [key: string]: string | number | boolean | null | undefined;
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

  private async request<T>(endpoint: string, retryCount: number = 2): Promise<ApiResponse<T>> {
    let lastError: Error | null = null;
    const fullUrl = `${this.baseUrl}${endpoint}`;

    for (let attempt = 0; attempt <= retryCount; attempt++) {
      try {
        const response = await fetch(fullUrl);
        
        if (!response.ok) {
          // 404 에러의 경우 재시도하지 않음
          if (response.status === 404) {
            return {
              success: false,
              message: `데이터를 찾을 수 없습니다. 스크리너가 실행되지 않았거나 결과 파일이 생성되지 않았을 수 있습니다.`
            };
          }
          
          // 서버 에러의 경우 재시도
          if (response.status >= 500 && attempt < retryCount) {
            console.warn(`API request failed (attempt ${attempt + 1}/${retryCount + 1}): ${response.status}`);
            await new Promise(resolve => setTimeout(resolve, 1000 * (attempt + 1))); // 지수 백오프
            continue;
          }
          
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        // 데이터 유효성 검증
        if (data && !data.success && data.message) {
          console.warn(`API returned error: ${data.message}`);
          return {
            success: false,
            message: this.getFriendlyErrorMessage(data.message)
          };
        }
        
        // 빈 데이터 검증
        if (data && data.success && data.data && Array.isArray(data.data) && data.data.length === 0) {
          return {
            success: true,
            data: data.data,
            message: '현재 조건에 맞는 종목이 없습니다.',
            total_count: 0,
            last_updated: data.last_updated
          };
        }
        
        return data;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error('Unknown error occurred');
        console.error(`API request error for ${fullUrl}:`, error);
        
        if (error instanceof Error) {
          console.error(`Error type: ${error.constructor.name}`);
          console.error(`Error message: ${error.message}`);
          console.error(`Error stack:`, error.stack);
        } else {
          console.error(`Error type: ${typeof error}`);
          console.error(`Error message: ${String(error)}`);
          console.error(`Error stack: N/A`);
        }
        
        if (attempt < retryCount) {
          console.warn(`API request failed (attempt ${attempt + 1}/${retryCount + 1}):`, lastError.message);
          await new Promise(resolve => setTimeout(resolve, 1000 * (attempt + 1))); // 지수 백오프
        }
      }
    }
    
    console.error('API request failed after all retries:', lastError);
    return {
      success: false,
      message: this.getFriendlyErrorMessage(lastError?.message || 'Unknown error occurred')
    };
  }
  
  private getFriendlyErrorMessage(errorMessage: string): string {
    if (errorMessage.includes('not found') || errorMessage.includes('404')) {
      return '요청한 데이터를 찾을 수 없습니다. 스크리너를 먼저 실행해주세요.';
    }
    if (errorMessage.includes('Failed to fetch') || errorMessage.includes('NetworkError')) {
      return '서버에 연결할 수 없습니다. 네트워크 연결을 확인해주세요.';
    }
    if (errorMessage.includes('timeout')) {
      return '요청 시간이 초과되었습니다. 잠시 후 다시 시도해주세요.';
    }
    if (errorMessage.includes('500') || errorMessage.includes('Internal Server Error')) {
      return '서버 내부 오류가 발생했습니다. 잠시 후 다시 시도해주세요.';
    }
    return errorMessage;
  }

  // 기술적 스크리닝 결과 (백엔드 엔드포인트에 맞춤)
  async getTechnicalResults() {
    return this.request<ScreeningData[]>('/api/screening-results');
  }

  // 스크리닝 결과 (getTechnicalResults와 동일한 엔드포인트)
  async getScreeningResults() {
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

  async getScreenerDescription(name: string) {
    return this.request<string>(`/api/screener-description/${name}`);
  }

  async getLeaderStockResults() {
    return this.request<ScreeningData[]>('/api/leader-stock');
  }

  async getMomentumSignalsResults() {
    return this.request<ScreeningData[]>('/api/momentum-signals');
  }

  // Qullamaggie screener endpoints
  async getQullamaggieBreakout() {
    return this.request<ScreeningData[]>('/api/qullamaggie/breakout');
  }

  async getQullamaggieEpisodePivot() {
    return this.request<ScreeningData[]>('/api/qullamaggie/episode-pivot');
  }

  async getQullamaggieParabolicShort() {
    return this.request<ScreeningData[]>('/api/qullamaggie/parabolic-short');
  }

  async getQullamaggieBuySignals() {
    return this.request<ScreeningData[]>('/api/qullamaggie/buy-signals');
  }

  async getQullamaggieSellSignals() {
    return this.request<ScreeningData[]>('/api/qullamaggie/sell-signals');
  }
 
  // Recent signals endpoint
  async getRecentSignals(days: number = 5) {
    return this.request<ScreeningData[]>(`/api/recent-signals?days=${days}`);
  }

  // Dashboard summary endpoint
  async getDashboardSummary() {
    return this.request<Record<string, unknown>>('/api/dashboard-summary');
  }

  // Mark Minervini screener endpoints
  async getMarkminerviniResults(screenerName: string) {
    return this.request<ScreeningData[]>(`/api/markminervini/${screenerName}`);
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
        strategies: {}
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

      // Legacy Portfolio Strategies 1~6은 더 이상 사용하지 않으므로
      // 대시보드 요약에서도 제외한다.

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
