# VML tunnel

`sshuttle-vml.service` is based on `template.sshuttle.service` and runs on
`blanco` as `rin`. It defines `USERNAME=ansible`, `HOSTNAME=vml.pub`, and
`HOST_INT_IP=10.255.0.1` directly in its `Environment` entries, forwarding TCP traffic to
`${HOST_INT_IP}/32` through `${USERNAME}@${HOSTNAME}`. The proxy listens on
localhost. SSH uses rin's existing configuration and key; firewall setup uses
the existing passwordless sudo access. SSH keepalives detect broken connections,
and systemd restarts the tunnel after failures.

`vml.pub` resolves to the public SSH endpoint. The `vml` alias resolves to the
private tunneled address, so it cannot be used to establish the tunnel itself.

`vml-loopback.service` runs on the remote host. It adds `10.255.0.1/32`
(the configured `HOST_INT_IP`) to `lo` at boot. If that value changes, update
this unit too. No physical network interface configuration is changed.

To install the current configuration from the repository root:

```bash
ssh ansible@vml.pub 'sudo -n install -m 0644 /dev/stdin /etc/systemd/system/vml-loopback.service && sudo -n systemctl daemon-reload && sudo -n systemctl enable --now vml-loopback.service' < sshuttle/vml-loopback.service
sudo install -m 0644 sshuttle/sshuttle-vml.service /etc/systemd/system/sshuttle-vml.service
sudo systemctl daemon-reload
sudo systemctl enable --now sshuttle-vml.service
```

After editing the local unit (including its connection variables), reinstall it,
run `sudo systemctl daemon-reload`, then
`sudo systemctl restart sshuttle-vml.service`.

Check status and connectivity:

```bash
systemctl status sshuttle-vml.service
journalctl -u sshuttle-vml.service -n 30 --no-pager
ssh -F /dev/null -i /home/rin/.ssh/id-vibeml.ansible \
  -o BatchMode=yes -o ConnectTimeout=10 \
  -o HostKeyAlias=vml -o StrictHostKeyChecking=yes \
  ansible@10.255.0.1 hostname
```

The SSH check reuses the known host key for `vml` and should print `vm-medium`.
Use TCP checks: this NAT tunnel does not forward ICMP (ping) or general UDP.
Both services are enabled for boot; reboot behavior has not been tested.

To remove the tunnel and private address:

```bash
sudo systemctl disable --now sshuttle-vml.service
ssh ansible@vml.pub 'sudo -n systemctl disable --now vml-loopback.service'
```
