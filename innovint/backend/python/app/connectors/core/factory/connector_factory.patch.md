# ConnectorFactory Registration Patch

This file shows the exact changes needed in PipesHub's `connector_factory.py`
to register the InnoVint connector.  Apply these changes to the PipesHub repo —
they cannot live in this connector repo because `connector_factory.py` is owned
by the PipesHub core team.

---

## File to edit

```
backend/python/app/connectors/core/factory/connector_factory.py
```

---

## Step 1 — Add the import

Find the block where other connectors are imported, for example near:

```python
from app.connectors.sources.microsoft.connector import MicrosoftConnector
from app.connectors.sources.google.connector import GoogleConnector
# … other connectors …
```

Add **one line** immediately after the last connector import:

```python
from app.connectors.sources.innovint.connector import InnovintConnector
```

---

## Step 2 — Register in the factory map

Find the `_CONNECTOR_MAP` dict (or equivalent registry) that maps connector
IDs / app-group names to their connector classes.  It will look something like:

```python
_CONNECTOR_MAP: dict[str, type[BaseConnector]] = {
    "microsoft": MicrosoftConnector,
    "google":    GoogleConnector,
    # … other connectors …
}
```

Add one entry:

```python
    "innovint": InnovintConnector,
```

---

## Step 3 — Add constants to arangodb.py  *(PipesHub repo)*

`InnovintApp` and `InnovintConnector` reference two constants that must exist
in `app/config/constants/arangodb.py`:

```python
# In the Connectors enum / namespace:
INNOVINT = "innovint"

# In the AppGroup enum / namespace:
INNOVINT = "InnoVint"
```

The exact form depends on whether `Connectors` and `AppGroup` are Python enums
or plain classes with string attributes — match the style used by existing
connectors such as `MICROSOFT` or `GOOGLE`.

---

## Complete diff (illustrative)

```diff
--- a/backend/python/app/connectors/core/factory/connector_factory.py
+++ b/backend/python/app/connectors/core/factory/connector_factory.py
@@ -5,6 +5,7 @@
 from app.connectors.sources.microsoft.connector import MicrosoftConnector
 from app.connectors.sources.google.connector    import GoogleConnector
+from app.connectors.sources.innovint.connector  import InnovintConnector

 _CONNECTOR_MAP: dict[str, type[BaseConnector]] = {
     "microsoft": MicrosoftConnector,
     "google":    GoogleConnector,
+    "innovint":  InnovintConnector,
 }
```

```diff
--- a/backend/python/app/config/constants/arangodb.py
+++ b/backend/python/app/config/constants/arangodb.py
@@ -12,6 +12,7 @@ class Connectors:
     MICROSOFT = "microsoft"
     GOOGLE    = "google"
+    INNOVINT  = "innovint"

@@ -22,6 +23,7 @@ class AppGroup:
     MICROSOFT = "Microsoft"
     GOOGLE    = "Google"
+    INNOVINT  = "InnoVint"
```

---

## Verification

After applying the patch, start the Python connector service and send a
test `innovint.resync` event to the `sync-events` Kafka topic:

```bash
# Using kafkacat / kcat
echo '{"event":"innovint.resync","orgId":"test-org","syncType":"full"}' \
  | kcat -P -b localhost:9092 -t sync-events -k test-org
```

The connector service log should show:

```
INFO  InnovintConnector  Starting full sync for org=test-org …
INFO  InnovintConnector  Synced N lots documents
INFO  InnovintConnector  Synced N vessels documents
…
```

And ArangoDB should contain new `Record` documents in the
`records` collection with `origin.connector == "innovint"`.
