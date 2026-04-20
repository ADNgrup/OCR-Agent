import Link from "next/link";

export default function SettingsHeader() {
  return (
    <div
      style={{
        display: "flex",
        justifyContent: "space-between",
        alignItems: "center",
        marginBottom: "20px",
      }}
    >
      <h1>System Settings</h1>
      <p>
        Configure LLM, pipeline, and prompt parameters. Database settings are
        read-only (set via <code>.env</code>).
      </p>
      <Link href="/" className="btn-sm btn-secondary">
        &larr; Back to Dashboard
      </Link>
    </div>
  );
}
