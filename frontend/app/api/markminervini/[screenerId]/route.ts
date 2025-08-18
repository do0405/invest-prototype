import { NextRequest, NextResponse } from 'next/server';

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:5000';

export async function GET(
  request: NextRequest,
  { params }: { params: { screenerId: string } }
) {
  try {
    const { screenerId } = params;
    
    // Backend API 호출
    const response = await fetch(`${BACKEND_URL}/api/markminervini/${screenerId}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
      // 캐시 비활성화로 최신 데이터 보장
      cache: 'no-store'
    });

    if (!response.ok) {
      console.error(`Backend API error: ${response.status} ${response.statusText}`);
      return NextResponse.json(
        { 
          success: false, 
          error: `Backend API error: ${response.status} ${response.statusText}`,
          message: 'Failed to fetch data from backend'
        }, 
        { status: response.status }
      );
    }

    const data = await response.json();
    
    // 백엔드에서 이미 필터링된 데이터를 그대로 반환
    return NextResponse.json(data);
    
  } catch (error) {
    console.error('API route error:', error);
    return NextResponse.json(
      { 
        success: false, 
        error: error instanceof Error ? error.message : 'Unknown error',
        message: 'Internal server error'
      }, 
      { status: 500 }
    );
  }
}