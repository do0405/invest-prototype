declare namespace JSX {
  interface IntrinsicElements {
    [elemName: string]: any;
  }
}

declare module 'react' {
  export type FC<P = any> = (props: P) => any;
  export const useState: any;
  export const useEffect: any;
  const React: {
    useState: any;
    useEffect: any;
  };
  export default React;
}

declare module 'next/link' {
  const Link: any;
  export default Link;
}

declare module 'framer-motion' {
  export const motion: any;
}

declare module 'react-icons/fa' {
  export const FaTimes: any;
  export const FaHome: any;
  export const FaChevronDown: any;
  export const FaChevronRight: any;
}

