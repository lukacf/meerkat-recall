-- Add inline payload storage for managed media assets so workers can load them
-- without relying on process-local filesystem state.

DEFINE FIELD payload_base64 ON TABLE media_asset TYPE option<string>;
