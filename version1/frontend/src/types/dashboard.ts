/* eslint-disable @typescript-eslint/no-explicit-any */

export interface KvmSource {
  id: string;
  name: string;
  host: string;
  port: number;
  enabled: boolean;
  poll_seconds: number;
  last_polled_at?: string;
}

export interface Screen {
  id: string;
  name: string;
  ignored?: boolean;
}

export interface Indicator {
  indicator_label?: string;
  display_name?: string;
  metric?: string;
  metric_key?: string;
  value_type?: string;
  last_value?: string | number;
  unit?: string;
}

export interface Entity {
  id: string; // Used to identify the entity UI-wise
  display_name?: string;
  main_entity_name?: string;
  entity_type?: string;
  type?: string;
  region?: string;
  indicators: {
    [metric_key: string]: {
      display_name?: string;
      indicator_label?: string;
      last_value: any;
      value_type: "text" | "numerical" | "boolean" | "color" | string;
      unit?: string;
      label?: string; // added back
      metric?: string; // missing prop added
      metric_key?: string; // missing prop added
    };
  };
  metrics?: Record<string, Indicator>;
  subentities?: any[];
  logs?: any[];
}

export interface LogEntry {
  log_id: string;
  recorded_at: string;
  entity_name?: string;
  entity_key?: string;
  metric?: string;
  value?: string | number;
  value_type?: string;
  unit?: string;
}

export interface QueueStats {
  pending: number;
  processing: number;
  completed: number;
  failed: number;
}

export interface Preview {
  snapshot_id: string;
  created_at: string;
  image_url: string;
}

export interface Dashboard {
  entity_id: string; // The selected entity's ID
  indicators: {
    [metric_key: string]: {
      display_name?: string;
      indicator_label?: string;
      last_value: any;
      value_type: "text" | "numerical" | "boolean" | "color" | string;
      unit?: string;
      label?: string; // added back
      metric?: string; // missing prop added
      metric_key?: string; // missing prop added
    };
  };
}
