import SearchForm from "@/components/SearchForm";

export default function Home() {
  return (
    <div className="min-h-screen">
      <header className="sticky top-0 z-50 border-b border-warm-200 bg-white/80 backdrop-blur-md">
        <div className="mx-auto max-w-7xl px-4 py-4 sm:px-6">
          <h1 className="text-xl font-semibold text-warm-900">
            Hospital Price Transparency
          </h1>
          <p className="text-sm text-warm-500">
            Search nearby hospitals by zip code and billing code
          </p>
        </div>
      </header>
      <main className="mx-auto max-w-7xl px-4 py-6 sm:px-6">
        <SearchForm />
      </main>
    </div>
  );
}
