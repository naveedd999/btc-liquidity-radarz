import './globals.css';
import type { Metadata } from 'next';
export const metadata: Metadata = { title: 'BTC Liquidity Radar', description: 'BTC/USDT market microstructure analytics terminal' };
export default function RootLayout({children}:{children:React.ReactNode}){return <html lang="en"><body>{children}</body></html>}
