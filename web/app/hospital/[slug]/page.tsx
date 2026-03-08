import HospitalDetail from "@/components/HospitalDetail";
import Link from "next/link";

export default async function HospitalPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  return (
    <div className="min-h-screen">
      <header className="sticky top-0 z-50 border-b border-warm-200 bg-white/80 backdrop-blur-md">
        <div className="mx-auto max-w-7xl px-4 py-4 sm:px-6">
          <Link
            href="/"
            className="text-xl font-semibold text-warm-900 hover:text-blue-700"
          >
            Hospital Price Transparency
          </Link>
          <p className="text-sm text-warm-500">Hospital detail</p>
        </div>
      </header>
      <main className="mx-auto max-w-7xl px-4 py-6 sm:px-6">
        <HospitalDetail slug={slug} />
      </main>
    </div>
  );
}
