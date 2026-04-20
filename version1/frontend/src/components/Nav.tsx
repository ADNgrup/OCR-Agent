"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

interface NavItem {
  href: string;
  label: string;
}

const NAV_ITEMS: NavItem[] = [
  { href: "/", label: "Dashboard" },
  { href: "/queue-details", label: "Queue Details" },
  { href: "/settings", label: "Settings" },
];

export default function Nav() {
  const pathname = usePathname();

  return (
    <nav className="sticky top-0 z-50 border-b  pad-2 border-gray-200 bg-white/85 backdrop-blur">
      <div className="flex h-13 w-full items-center gap-6 px-5">
        <span className="text-base font-extrabold tracking-tight text-blue-700">
          KVM OCR
        </span>
        <ul className="flex list-none gap-1 p-0">
          {NAV_ITEMS.map((item) => (
            <li key={item.href}>
              <Link
                href={item.href}
                className={
                  "inline-block rounded-lg px-3.5 py-1.5 text-[13px] font-semibold no-underline transition-colors " +
                  (pathname === item.href
                    ? "bg-blue-700 text-white"
                    : "text-gray-500 hover:bg-indigo-50 hover:text-blue-700")
                }
              >
                {item.label}
              </Link>
            </li>
          ))}
        </ul>
      </div>
    </nav>
  );
}
