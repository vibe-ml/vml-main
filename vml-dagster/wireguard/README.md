# WireGuard: vml.pub ↔ blanco

For the vml.pub ↔ pdc-lite.pub tunnel (`10.255.2.5` ↔ `10.255.2.6`), see
[pdc-lite setup](pdc-lite-setup.md) and the `vml-pdc.conf` / `pdc-lite.conf` templates.

| Host | Tunnel address | Configuration |
| --- | --- | --- |
| vml.pub | `10.255.2.1` | `vml.conf` |
| blanco | `10.255.2.2` | `blanco.conf` |

These are configuration templates: replace the key placeholders in the installed
copies before starting WireGuard. Keep private keys out of this repository.
Only traffic to the other host's tunnel address is routed through WireGuard.
No IP forwarding, masquerading, or default-route changes are required for
host-to-host connections.

## Install and generate keys

On each host, install WireGuard tools (Ubuntu/Debian):

```sh
sudo apt-get update
sudo apt-get install wireguard-tools
```

Generate a separate key pair on each host. Run once; do not overwrite existing
keys or an existing `wg0.conf` deployment:

```sh
sudo install -d -m 700 /etc/wireguard
sudo sh -c 'umask 077; wg genkey > /etc/wireguard/private.key; wg pubkey < /etc/wireguard/private.key > /etc/wireguard/public.key'
sudo cat /etc/wireguard/public.key
```

Exchange only the public keys between hosts.

## Configure

Copy the appropriate template to its host, then install it:

```sh
# On vml.pub, from the directory containing vml.conf:
sudo install -m 600 vml.conf /etc/wireguard/wg0.conf

# On blanco, from the directory containing blanco.conf:
sudo install -m 600 blanco.conf /etc/wireguard/wg0.conf
```

On each host, use `sudoedit /etc/wireguard/wg0.conf` to set:

- `PrivateKey`: that host's `/etc/wireguard/private.key` contents.
- `PublicKey` in `[Peer]`: the other host's `/etc/wireguard/public.key` contents.

Remove the angle brackets along with the placeholder names. Leave the templates
in the repository unchanged.

Before starting, check that `10.255.2.1` and `10.255.2.2` do not conflict with
existing networks on either host. Permit inbound **UDP 51820** on vml's host and
provider firewalls. Blanco must be able to send UDP to `vml.pub:51820` and receive
the replies. Its keepalive maintains the NAT mapping; no router port forwarding
on blanco is needed for this arrangement.

Allow the desired TCP service ports on interface `wg0` from the peer's tunnel IP
in each host's firewall. Services must listen on the tunnel IP or an appropriate
wildcard address; a service bound only to localhost is not reachable through the
tunnel. Container services additionally need suitable published ports or routing.

## Start and verify

On both hosts:

```sh
sudo systemctl enable --now wg-quick@wg0
sudo wg show wg0
```

Check for a recent handshake after both hosts have started. Test TCP connections
to an actual listening service (replace `PORT` with its port number):

```sh
# From blanco:
nc -vz 10.255.2.1 PORT

# From vml.pub:
nc -vz 10.255.2.2 PORT
```

Use the tunnel addresses for application connections. `vml.pub` continues to
resolve to its public address and is used as the tunnel's outer endpoint.

Blanco's service on port `30004` exposes a health endpoint. From vml.pub:

```sh
curl --noproxy '*' --fail --max-time 10 http://10.255.2.2:30004/health
```

A healthy service returns HTTP 200 at `/health`; its root path `/` returns
HTTP 404. Ensure vml's Cloud.ru security group permits inbound UDP 51820
from blanco's public egress IP, in addition to any host firewall rules.

The deployed configurations live in `/etc/wireguard/wg0.conf` on both hosts,
owned by root with mode 600. The repository files remain templates without keys.

To stop the tunnel and disable startup on either host:

```sh
sudo systemctl disable --now wg-quick@wg0
```
