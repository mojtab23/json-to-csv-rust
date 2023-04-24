use std::fs::File;

use serde_json::Value;

fn main() {
    let file = File::open("C:\\Users\\Mojtaba\\Desktop\\duplicate-urldomains-min.json").unwrap();
    let json: Value = serde_json::from_reader(file).unwrap();
    let mut csv_writer = csv::WriterBuilder::new()
        .delimiter(b';')
        .from_path("C:\\Users\\Mojtaba\\Desktop\\duplicate-urldomains-min.csv")
        .unwrap();
    let buckets = json["aggregations"]["adbuk"]["buckets"].as_array().unwrap();
    let mut csv_id = 0;

    csv_writer
        .write_record(&[
            "csv_id",
            "key",
            "doc_count",
            "id",
            "index",
            "issuer_id",
            "created_date",
            "created_by",
            "white",
            "modified_date",
            "modified_by",
            "addressMatchType",
            "type",
            "communicationClassId",
        ])
        .unwrap();

    for entry in buckets {
        let key = entry["key"].as_str().unwrap();
        let doc_count = entry["doc_count"].as_i64().unwrap().to_string();

        let hits = entry["iptops"]["hits"]["hits"].as_array().unwrap();
        for hit in hits {
            let index = hit["_index"].as_str().unwrap();
            let id = hit["_id"].as_str().unwrap();
            let source = &hit["_source"];
            let issuer_id = source["issuerId"].as_str().unwrap();
            let created_date = source["createdDate"].as_i64().unwrap().to_string();
            let white = source["white"].as_bool().unwrap().to_string();
            let created_by = source["createdBy"].as_str().unwrap();
            let modified_date = source["modifiedDate"]
                .as_i64()
                .unwrap()
                .to_string();
            let amt = source["addressMatchType"].as_str().unwrap();
            let modified_by = source["modifiedBy"].as_str().unwrap();
            let r#type = source["type"].as_str().unwrap();
            let cci = source["communicationClassId"].as_str().unwrap();

            let csv_id_str = csv_id.to_string();

            csv_writer
                .write_record(&[
                    &csv_id_str,
                    key,
                    &doc_count,
                    id,
                    index,
                    issuer_id,
                    &created_date,
                    created_by,
                    &white,
                    &modified_date,
                    modified_by,
                    amt,
                    r#type,
                    cci,
                ])
                .unwrap();
        }
        csv_id += 1;

        // csv_writer.write_record(&[key, doc_count.to_string().as_str()]).unwrap();
    }
    return;
}
