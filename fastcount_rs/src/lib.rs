use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};

fn open_text_reader(path: &str) -> std::io::Result<Box<dyn Read>> {
    // 支持 .gz 与纯文本；如需 .zip 可后续扩展
    if path.ends_with(".gz") {
        let file = File::open(path)?;
        let decoder = flate2::read::GzDecoder::new(file);
        Ok(Box::new(decoder))
    } else {
        let file = File::open(path)?;
        Ok(Box::new(file))
    }
}

#[pyfunction]
fn parse_files(py: Python<'_>, paths: Vec<String>) -> PyResult<Py<PyDict>> {
    // 5 类 isoform 基础计数器
    let mut raw: HashMap<String, u64> = HashMap::new();
    let mut first_id: HashMap<String, u64> = HashMap::new();
    let mut unique: HashMap<String, u64> = HashMap::new();
    let mut multi_isoform: HashMap<String, u64> = HashMap::new();
    let mut multi2all: HashMap<String, u64> = HashMap::new();

    for p in paths.iter() {
        let reader = open_text_reader(p).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("open {} failed: {}", p, e)))?;
        let mut buf = BufReader::new(reader);
        let mut line1 = String::new();
        let mut line2 = String::new();

        loop {
            line1.clear();
            line2.clear();
            let n1 = buf.read_line(&mut line1).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("read line1 failed: {}", e)))?;
            let n2 = buf.read_line(&mut line2).map_err(|e| PyErr::new::<pyo3::exceptions::PyIOError, _>(format!("read line2 failed: {}", e)))?;
            if n1 == 0 || n2 == 0 { break; }
            let line1 = line1.trim_end();
            let line2 = line2.trim_end();
            let f1: Vec<&str> = line1.split('\t').collect();
            let f2: Vec<&str> = line2.split('\t').collect();
            if f1.len() < 2 || f2.len() < 5 { continue; }

            // 第二行字段：strand, ref_names, mismatch, positions, multi_count
            let ref_field = f2[1];
            let multi_count: i64 = match f2[4].parse() { Ok(v) => v, Err(_) => 0 };
            if multi_count != 1 {
                // 多重比对：组合键采用逗号分隔字符串（Python 侧兼容 tuple 或逗号字符串）
                let joined = ref_field.to_string();
                // raw: 组合键计数 +1
                *raw.entry(joined.clone()).or_insert(0) += 1;
                // firstID: 使用第一个 ref_name
                if let Some(first) = ref_field.split(',').next() {
                    *first_id.entry(first.to_string()).or_insert(0) += 1;
                }
                // multi_to_isoform: 组合键计数 +1
                *multi_isoform.entry(joined.clone()).or_insert(0) += 1;
                // multi2all: 展开到每个转录本 +1
                for tid in ref_field.split(',') {
                    *multi2all.entry(tid.to_string()).or_insert(0) += 1;
                }
            } else {
                // 唯一比对：ID 直接使用 ref_name
                let id = ref_field.to_string();
                *raw.entry(id.clone()).or_insert(0) += 1;
                *first_id.entry(id.clone()).or_insert(0) += 1;
                *unique.entry(id.clone()).or_insert(0) += 1;
            }
        }
    }

    // 构造返回的 Python 字典
    let out = PyDict::new(py);
    out.set_item("raw", raw)?;
    out.set_item("unique_to_isoform", unique)?;
    out.set_item("multi_to_isoform", multi_isoform)?;
    out.set_item("firstID", first_id)?;
    out.set_item("multi2all", multi2all)?;
    Ok(out.into_py(py))
}

#[pymodule]
fn fansetools_fastcount(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    // 模块初始化
    m.add_function(pyo3::wrap_pyfunction!(parse_files, m)?)?;
    Ok(())
}

