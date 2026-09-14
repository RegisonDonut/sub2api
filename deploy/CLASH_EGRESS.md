# Per-account Clash egress

Sub2API can keep OpenAI OAuth accounts on stable, independently switchable
Clash egress routes. Runtime credentials and subscription URLs must stay on
the deployment host and must not be committed.

## Clash/Mihomo

1. Import the OpenAI node subscription in Clash Verge.
2. Attach `clash-openai-egress.js` as that profile's script transform. The
   script preserves the existing `🤖 OpenAI` group and adds one authenticated
   listener and selector for every node admitted by that group.
3. Enable the external controller on `0.0.0.0:9097` with a random secret. Keep
   the host firewall closed to LAN access for controller and listener ports.
4. Store the same secret in a mode-0600 file. Mount it read-only into Sub2API;
   prefer `CLASH_EGRESS_SECRET_FILE` over placing the value in `.env`.

Listeners start at port `17901`. Their usernames are `slot1`, `slot2`, and so
on; their password is the controller secret. The number of listeners follows
the current `🤖 OpenAI` candidate count.

## Sub2API

Create proxy records named `clash-openai-slot-N` for healthy listeners, using
`host.docker.internal`, the corresponding `17900 + N` port, and listener
credentials. Keep unreachable listeners disabled. If multiple node names have
the same observed public IP, enable only one representative when distinct IP
assignment is required.

Set:

```dotenv
CLASH_EGRESS_ENABLED=true
CLASH_EGRESS_CONTROLLER_URL=http://host.docker.internal:9097
CLASH_EGRESS_SECRET_FILE=/run/secrets/clash-controller
```

New OpenAI OAuth accounts without an explicit proxy use the active managed
proxy with the lowest account count. When OpenAI explicitly rejects an egress
IP, Sub2API moves that account's selector to another live node and retries the
same account. Other errors continue through the normal account failover path.

## Host migration

Move database and Redis volumes through the normal Sub2API backup process.
Re-import the Clash subscription on the destination host, attach the script,
create a new local controller secret, update proxy credentials, and probe exit
IPs before enabling the pool. Do not copy a subscription URL or secret through
Git, container images, application logs, or Donut Work plugin configuration.
