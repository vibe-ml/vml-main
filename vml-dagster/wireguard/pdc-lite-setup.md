# WireGuard: vml.pub ↔ pdc-lite.pub

| Host         | Tunnel address  | Template        |
| ------------ | --------------- | --------------- |
| vml.pub      | `10.255.2.5/32` | `vml-pdc.conf`  |
| pdc-lite.pub | `10.255.2.6/32` | `pdc-lite.conf` |

Both hosts listen on UDP port `51820`. Only traffic to the other host's
tunnel address uses this tunnel. Allow inbound UDP `51820` on both hosts'
host and provider firewalls; if behind NAT, forward that port to the host.
Allow the desired services from the peer's tunnel address as well.

Install `wireguard-tools` on each host. Generate a separate key pair on each
host (or reuse the existing interface's key pair when adding a peer):

```sh
umask 077
wg genkey | tee private.key | wg pubkey > public.key
```

Run this in a private directory outside the repository, without overwriting
existing keys. Exchange public keys, then replace the template placeholders in
the installed copies with the corresponding private and peer public keys.

## Standalone installation

Use this procedure when no existing WireGuard interface uses port `51820`.
Copy each template to its host and install it:

```sh
# On vml.pub:
sudo install -D -m 600 vml-pdc.conf /etc/wireguard/wg-pdc.conf

# On pdc-lite.pub:
sudo install -D -m 600 pdc-lite.conf /etc/wireguard/wg-pdc.conf
```

Set the keys with `sudoedit /etc/wireguard/wg-pdc.conf`, then on both hosts:

```sh
sudo systemctl enable --now wg-quick@wg-pdc
sudo wg show wg-pdc
```

## Sharing vml's existing blanco interface

If vml already runs the blanco tunnel on `wg0` at port `51820`, add
`10.255.2.5/32` to that interface's existing `Address` list and append the
`[Peer]` section from `vml-pdc.conf` to `/etc/wireguard/wg0.conf`, substituting
pdc-lite's public key. Retain the existing private key, address, and blanco
peer. Configure pdc-lite with the public key of vml's existing `wg0` interface.
Do not start a second interface on vml using the same listen port.

Add this line to vml's `[Interface]` section so outgoing traffic uses the
address accepted by pdc-lite, rather than the existing blanco address:

```ini
PostUp = ip route replace 10.255.2.6/32 dev %i src 10.255.2.5
```

Apply the updated vml configuration during a suitable interruption window
with `sudo systemctl restart wg-quick@wg0`. Install and start pdc-lite's
configuration using the standalone steps above.

## Verify

From vml, run `ping -c 3 10.255.2.6`; from pdc-lite, run
`ping -c 3 10.255.2.5` (allow ICMP in the host firewalls for these checks).
Use `sudo wg show` to confirm a recent handshake and traffic counters.
Connect applications using the tunnel addresses. The templates use public IP
endpoints because the SSH aliases do not resolve on the remote hosts.

## Deployed configuration

Deployment verified on 2026-09-28:

- vml: `/etc/wireguard/wg0.conf`, service `wg-quick@wg0`, with the existing
  blanco peer retained. The new address, peer, and source route were applied
  live without restarting the interface.
- pdc-lite: `/etc/wireguard/wg-pdc.conf`, service `wg-quick@wg-pdc`.
- Both services are active and enabled at boot. Both configurations are owned
  by root with mode `600`; private keys remain on the hosts.
- Public endpoints: vml `176.108.243.127:51820`, pdc-lite
  `95.174.92.246:51820`.
- Four pings in each direction succeeded with zero packet loss and roughly
  1–1.4 ms round-trip latency. WireGuard reported a successful handshake and
  bidirectional transfer. The existing blanco peer also passed two pings.
- vml's original configuration backup:
  `/etc/wireguard/wg0.conf.bak-20260929-002603`.
