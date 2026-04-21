// Package storage 提供MVCC版本管理和行编解码功能
package storage

import (
	"encoding/binary"
)

// FlagDeleted 删除标记
// 用于MVCC值中标记行是否被删除
const (
	FlagDeleted byte = 0x01
)

// VersionKey 将主键和提交时间戳编码为B+树键
// 使用 ^commitTS（大端序）使新版本排在前面
// 格式：[PK][^commitTS]
func VersionKey(pk []byte, commitTS uint64) []byte {
	buf := make([]byte, len(pk)+8)
	copy(buf, pk)
	binary.BigEndian.PutUint64(buf[len(pk):], ^commitTS)
	return buf
}

// KeyPrefix 从版本键中提取主键部分
func KeyPrefix(key []byte) []byte {
	if len(key) <= 8 {
		return key
	}
	return key[:len(key)-8]
}

// KeyCommitTS 从版本键中提取提交时间戳
func KeyCommitTS(key []byte) uint64 {
	if len(key) < 8 {
		return 0
	}
	return ^binary.BigEndian.Uint64(key[len(key)-8:])
}

// compareKeys 比较两个字节切片
// 返回 -1, 0, 或 1
func compareKeys(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// EncodeMVCCValue 编码带MVCC元数据的行数据
// 格式：[1字节flags][8字节xmin][8字节xmax][行数据]
// xmin: 创建该版本的事务时间戳
// xmax: 删除该版本的事务时间戳（如果不为0表示已删除）
// flags: 标记位（如FlagDeleted）
func EncodeMVCCValue(xmin, xmax uint64, flags byte, rowData []byte) []byte {
	buf := make([]byte, 1+8+8+len(rowData))
	buf[0] = flags
	binary.BigEndian.PutUint64(buf[1:], xmin)
	binary.BigEndian.PutUint64(buf[9:], xmax)
	copy(buf[17:], rowData)
	return buf
}

// DecodeMVCCValue 解码MVCC元数据和行数据
func DecodeMVCCValue(data []byte) (xmin, xmax uint64, flags byte, rowData []byte, err error) {
	if len(data) < 17 {
		return 0, 0, 0, nil, errValueTooShort
	}
	flags = data[0]
	xmin = binary.BigEndian.Uint64(data[1:])
	xmax = binary.BigEndian.Uint64(data[9:])
	rowData = data[17:]
	return
}

var errValueTooShort = &mvccError{"mvcc value too short"}

type mvccError struct{ msg string }

func (e *mvccError) Error() string { return e.msg }

// IsVisible 判断行版本在给定读取时间戳下是否可见
// MVCC可见性规则（简化版，不再使用xmax）：
// 1. 如果设置了删除标记，则不可见
// 2. 如果xmin大于读取时间戳，说明在快照之后创建，不可见
func IsVisible(xmin, xmax uint64, flags byte, readTS uint64) bool {
	if flags&FlagDeleted != 0 {
		return false // 墓碑不可见
	}
	if xmin > readTS {
		return false // 在快照之后创建
	}
	return true
}

// ScanRangeForPK 返回用于扫描某个主键所有版本的起始和结束键
// 范围从最新版本到最老版本
func ScanRangeForPK(pk []byte) (start, end []byte) {
	// 开始：PK + 0x0000000000000000（最新可能 = 最大时间戳）
	// 结束：PK + 0xFFFFFFFFFFFFFFFF（最老可能 = 时间戳0）
	start = make([]byte, len(pk)+8)
	copy(start, pk)
	// start后缀 = 0x00...00 = ^uint64_max = 最大时间戳 -> 排序最前

	end = make([]byte, len(pk)+8)
	copy(end, pk)
	for i := len(pk); i < len(end); i++ {
		end[i] = 0xFF
	}
	return start, end
}
