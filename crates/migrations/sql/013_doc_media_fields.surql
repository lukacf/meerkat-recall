-- Add multimodal document fields used by media-backed docs.

DEFINE FIELD sidecar_text ON TABLE doc TYPE option<string>;
DEFINE FIELD media_attachments ON TABLE doc TYPE array<object> FLEXIBLE;
