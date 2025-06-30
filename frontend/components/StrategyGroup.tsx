import { useState } from 'react';
import Link from 'next/link';
import { motion } from 'framer-motion';
import { FaChevronDown, FaChevronRight } from 'react-icons/fa';

export interface StrategyGroupItem {
  id: string;
  name: string;
  icon?: string;
  type?: 'buy' | 'sell';
}

interface StrategyGroupProps {
  title: string;
  items: StrategyGroupItem[];
  basePath: string;
  defaultOpen?: boolean;
}

const MotionLink = motion(Link);

const StrategyGroup = ({ title, items, basePath, defaultOpen = false }: StrategyGroupProps) => {
  const [isOpen, setIsOpen] = useState(defaultOpen);

  return (
    <div className="mb-4">
      <motion.button
        onClick={() => setIsOpen(!isOpen)}
        className="w-full flex items-center justify-between py-2.5 px-4 rounded-md"
        whileHover={{ backgroundColor: 'rgba(129, 140, 248, 0.1)', color: '#4f46e5' }}
        whileTap={{ backgroundColor: 'rgba(129, 140, 248, 0.2)' }}
        transition={{ type: 'spring', stiffness: 400, damping: 17 }}
      >
        <span className="font-semibold">{title}</span>
        {isOpen ? <FaChevronDown size={16} /> : <FaChevronRight size={16} />}
      </motion.button>
      {isOpen && (
        <div className="ml-4 mt-2 space-y-1">
          {items.map(item => (
            <MotionLink
              key={item.id}
              href={`${basePath}/${item.id}`}
              className={`block py-2 px-4 rounded-md text-sm ${
                item.type === 'buy'
                  ? 'border-l-2 border-green-200'
                  : item.type === 'sell'
                  ? 'border-l-2 border-red-200'
                  : ''
              }`}
              whileHover={{
                backgroundColor:
                  item.type === 'buy'
                    ? 'rgba(34, 197, 94, 0.1)'
                    : item.type === 'sell'
                    ? 'rgba(239, 68, 68, 0.1)'
                    : 'rgba(129, 140, 248, 0.1)',
                color:
                  item.type === 'buy' ? '#16a34a' : item.type === 'sell' ? '#dc2626' : '#4f46e5',
                borderLeftColor:
                  item.type === 'buy' ? '#4ade80' : item.type === 'sell' ? '#f87171' : '#c7d2fe'
              }}
              whileTap={{
                backgroundColor:
                  item.type === 'buy'
                    ? 'rgba(34, 197, 94, 0.2)'
                    : item.type === 'sell'
                    ? 'rgba(239, 68, 68, 0.2)'
                    : 'rgba(129, 140, 248, 0.2)'
              }}
              transition={{ type: 'spring', stiffness: 400, damping: 17 }}
            >
              {item.icon ? `${item.icon} ${item.name}` : item.name}
            </MotionLink>
          ))}
        </div>
      )}
    </div>
  );
};

export default StrategyGroup;
