import React from 'react';

interface TableSkeletonProps {
  rows?: number;
  columns?: number;
  className?: string;
}

function TableSkeleton({ rows = 5, columns = 4, className = '' }: TableSkeletonProps) {
  return (
    <div className={`animate-pulse ${className}`}>
      {/* 테이블 헤더 스켈레톤 */}
      <div className="bg-muted px-6 py-3 border-b border-border">
        <div className="flex space-x-4">
          {Array.from({ length: columns }).map((_, index) => (
            <div key={index} className="flex-1">
              <div className="h-4 bg-muted-foreground/20 rounded-notion w-3/4"></div>
            </div>
          ))}
        </div>
      </div>
      
      {/* 테이블 바디 스켈레톤 */}
      <div className="bg-card divide-y divide-border">
        {Array.from({ length: rows }).map((_, rowIndex) => (
          <div key={rowIndex} className="px-6 py-4">
            <div className="flex space-x-4">
              {Array.from({ length: columns }).map((_, colIndex) => (
                <div key={colIndex} className="flex-1">
                  <div className="h-4 bg-muted-foreground/10 rounded-notion w-full"></div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default TableSkeleton;