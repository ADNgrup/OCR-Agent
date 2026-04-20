interface SettingsStatusCardProps {
  status: string;
  error: string;
}

export default function SettingsStatusCard({ status, error }: SettingsStatusCardProps) {
  if (!status && !error) return null;

  return (
    <section className="card status-card">
      {status && (
        <div>
          <strong>Status:</strong> {status}
        </div>
      )}
      {error && <p className="error">{error}</p>}
    </section>
  );
}
