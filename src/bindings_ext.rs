use std::fmt;

use nix::{libc, request_code_read, request_code_readwrite};

use crate::bindings::{
    ublk_params, ublksrv_ctrl_cmd, ublksrv_ctrl_dev_info, ublksrv_io_cmd,
    ublksrv_io_desc,
};

pub const UBLK_U_CMD_GET_DEV_INFO: u32 =
    request_code_read!(b'u', 0x02, size_of::<ublksrv_ctrl_cmd>()) as u32;

pub const UBLK_U_CMD_ADD_DEV: u32 =
    request_code_readwrite!(b'u', 0x04, size_of::<ublksrv_ctrl_cmd>())
        as u32;

pub const UBLK_U_CMD_START_DEV: u32 =
    request_code_readwrite!(b'u', 0x06, size_of::<ublksrv_ctrl_cmd>())
        as u32;

pub const UBLK_U_CMD_STOP_DEV: u32 =
    request_code_readwrite!(b'u', 0x07, size_of::<ublksrv_ctrl_cmd>())
        as u32;

pub const UBLK_U_CMD_SET_PARAMS: u32 =
    request_code_readwrite!(b'u', 0x08, size_of::<ublksrv_ctrl_cmd>())
        as u32;

pub const UBLK_U_CMD_START_USER_RECOVERY: u32 =
    request_code_readwrite!(b'u', 0x10, size_of::<ublksrv_ctrl_cmd>())
        as u32;

pub const UBLK_U_CMD_END_USER_RECOVERY: u32 =
    request_code_readwrite!(b'u', 0x11, size_of::<ublksrv_ctrl_cmd>())
        as u32;

pub const UBLK_U_CMD_DEL_DEV_ASYNC: u32 =
    request_code_read!(b'u', 0x14, size_of::<ublksrv_ctrl_cmd>()) as u32;

pub const UBLK_U_IO_FETCH_REQ: u32 =
    request_code_readwrite!(b'u', 0x20, size_of::<ublksrv_io_cmd>())
        as u32;
pub const UBLK_U_IO_COMMIT_AND_FETCH_REQ: u32 =
    request_code_readwrite!(b'u', 0x21, size_of::<ublksrv_io_cmd>())
        as u32;

pub const UBLK_IO_RES_ABORT: i32 = -libc::ENODEV;

impl ublksrv_ctrl_dev_info {
    #[inline(always)]
    pub fn len() -> u16 {
        size_of::<ublksrv_ctrl_dev_info>() as u16
    }
}

impl ublk_params {
    #[inline(always)]
    pub fn len() -> u16 {
        size_of::<ublk_params>() as u16
    }
}

impl ublksrv_io_desc {
    #[inline(always)]
    pub fn op(&self) -> u32 {
        self.op_flags & 0xff
    }

    #[inline(always)]
    pub fn flags(&self) -> u32 {
        self.op_flags >> 8
    }
}

impl fmt::Debug for ublksrv_io_desc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nr_sectors = unsafe { self.__bindgen_anon_1.nr_sectors };
        let nr_zones = unsafe { self.__bindgen_anon_1.nr_zones };

        f.debug_struct("ublksrv_io_desc")
            .field("op_flags", &self.op_flags)
            .field("op", &self.op())
            .field("flags", &self.flags())
            .field("start_sector", &self.start_sector)
            .field("addr", &self.addr)
            .field("nr_sectors", &nr_sectors)
            .field("nr_zones", &nr_zones)
            .field("addr", &self.addr)
            .finish()
    }
}
