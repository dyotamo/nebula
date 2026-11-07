use std::{io, time::Duration};

use nebula_health::{
    lifecycle::{ApplicationState, Lifecycle},
    ApplicationHealth, ComponentHealth, HealthManager,
};
use tokio::sync::watch;

pub fn notify(message: &str) -> io::Result<bool> {
    let Ok(socket_path) = std::env::var("NOTIFY_SOCKET") else {
        return Ok(false);
    };
    let path_bytes = socket_path.as_bytes();
    let abstract_socket = path_bytes.first() == Some(&b'@');
    let address_bytes = if abstract_socket {
        &path_bytes[1..]
    } else {
        path_bytes
    };
    let path_capacity =
        std::mem::size_of_val(&unsafe { std::mem::zeroed::<libc::sockaddr_un>() }.sun_path);
    let required = address_bytes.len() + 1;
    if required > path_capacity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "systemd notification socket path is too long",
        ));
    }

    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let mut address = unsafe { std::mem::zeroed::<libc::sockaddr_un>() };
    address.sun_family = libc::AF_UNIX as libc::sa_family_t;
    if abstract_socket {
        address.sun_path[0] = 0;
        for (index, byte) in address_bytes.iter().enumerate() {
            address.sun_path[index + 1] = *byte as libc::c_char;
        }
    } else {
        for (index, byte) in address_bytes.iter().enumerate() {
            address.sun_path[index] = *byte as libc::c_char;
        }
        address.sun_path[address_bytes.len()] = 0;
    }
    let path_length = std::mem::size_of_val(&address.sun_family) + required;
    let address_length = path_length as libc::socklen_t;
    let sent = unsafe {
        libc::sendto(
            fd,
            message.as_ptr().cast(),
            message.len(),
            0,
            (&address as *const libc::sockaddr_un).cast(),
            address_length,
        )
    };
    let send_error = (sent < 0).then(io::Error::last_os_error);
    unsafe { libc::close(fd) };
    if let Some(error) = send_error {
        Err(error)
    } else if sent as usize != message.len() {
        Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "short write to systemd notification socket",
        ))
    } else {
        Ok(true)
    }
}

pub fn watchdog_interval() -> Option<Duration> {
    let usec = std::env::var("WATCHDOG_USEC").ok()?.parse::<u64>().ok()?;
    if usec == 0 {
        return None;
    }
    if let Ok(pid) = std::env::var("WATCHDOG_PID") {
        if pid.parse::<u32>().ok() != Some(std::process::id()) {
            return None;
        }
    }
    Some(Duration::from_micros(usec / 2).max(Duration::from_millis(100)))
}

pub async fn run_watchdog(
    lifecycle: Lifecycle,
    health: HealthManager,
    mut updates: watch::Receiver<ApplicationState>,
    interval: Duration,
) {
    let mut ticks = tokio::time::interval(interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticks.tick() => {
                if lifecycle.state() == ApplicationState::Ready
                    && health.snapshot().overall == ApplicationHealth::Healthy
                    && health.snapshot().components.values().all(|status| *status == ComponentHealth::Healthy)
                {
                    if let Err(error) = notify("WATCHDOG=1") {
                        tracing::warn!(error = %error, "systemd watchdog notification failed");
                    }
                }
            }
            changed = updates.changed() => {
                if changed.is_err() || matches!(
                    *updates.borrow(),
                    ApplicationState::Stopping | ApplicationState::Stopped | ApplicationState::Failed
                ) {
                    return;
                }
            }
        }
    }
}
