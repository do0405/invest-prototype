import React from 'react';

export interface DataTableColumn<T> {
  key: string;
  header: React.ReactNode;
  align?: 'left' | 'right' | 'center';
  render?: (item: T) => React.ReactNode;
}

interface DataTableProps<T> {
  data: T[];
  columns: DataTableColumn<T>[];
  rowKey?: (item: T, index: number) => React.Key;
  className?: string;
  headerRowClassName?: string;
  responsive?: boolean;
  cardClassName?: string;
}

function DataTable<T extends Record<string, any>>({ data, columns, rowKey,
  className = '', headerRowClassName = '', responsive = true,
  cardClassName = 'bg-white rounded-lg shadow p-4 mb-4' }: DataTableProps<T>) {
  const getAlignClass = (align?: string) => {
    switch (align) {
      case 'right':
        return 'text-right';
      case 'center':
        return 'text-center';
      default:
        return 'text-left';
    }
  };

  return (
    <div className={`overflow-x-auto ${className}`}>
      <table className="min-w-full hidden sm:table">
        <thead>
          <tr className={headerRowClassName}>
            {columns.map(col => (
              <th
                key={col.key}
                className={`px-4 py-3 text-xs font-medium text-gray-500 uppercase tracking-wider ${getAlignClass(col.align)}`}
              >
                {col.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {data.map((item, idx) => (
            <tr key={rowKey ? rowKey(item, idx) : idx} className="hover:bg-gray-50">
              {columns.map(col => (
                <td
                  key={col.key}
                  className={`px-4 py-4 whitespace-nowrap text-sm ${getAlignClass(col.align)}`}
                >
                  {col.render ? col.render(item) : String(item[col.key] ?? 'N/A')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {responsive && (
        <div className="sm:hidden">
          {data.map((item, idx) => (
            <div key={rowKey ? rowKey(item, idx) : idx} className={cardClassName}>
              {columns.map(col => (
                <div key={col.key} className="flex justify-between py-1">
                  <span className="font-medium mr-4">{col.header}</span>
                  <span className={`${getAlignClass(col.align)} whitespace-nowrap`}>
                    {col.render ? col.render(item) : String(item[col.key] ?? 'N/A')}
                  </span>
                </div>
              ))}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default DataTable;
