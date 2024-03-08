use std::ops::Index;

use yaml_rust::YamlLoader;

pub struct Configure {
    pub(crate) peer_cnt: usize,
    pub(crate) peer: Vec<String>,
    pub(crate) index: usize,
    pub(crate) epoch: usize,
}

impl Configure {
    pub fn new(peer_cnt: usize, peer: Vec<String>, index: usize, epoch: usize) -> Self {
        // TODO: Maybe we can start node on random numbers.
        if (peer_cnt % 2) == 0 {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }

        Self {
            peer_cnt,
            peer,
            index,
            epoch,
        }
    }
}

impl Index<usize> for Configure {
    type Output = str;

    fn index(&self, index: usize) -> &Self::Output {
        &self.peer[index]
    }
}

pub trait ConfigureSrc {
    fn get_configure(&self) -> Configure;
}

pub struct YamlConfigureSrc {
    yaml: String,
}

impl YamlConfigureSrc {
    pub fn new(yaml: &str) -> Self {
        Self {
            yaml: yaml.to_owned(),
        }
    }
}

impl ConfigureSrc for YamlConfigureSrc {
    fn get_configure(&self) -> Configure {
        let yaml = YamlLoader::load_from_str(&self.yaml);
        match yaml {
            Ok(y) => {
                if y.len() != 1 {
                    panic!("We should only pass in a yaml file");
                }

                let yaml = y.get(0).unwrap();

                let peer_cnt = yaml["peer_cnt"].as_i64().unwrap() as usize;
                let peer = yaml["peer"]
                    .as_vec()
                    .unwrap()
                    .iter()
                    .map(|y| y.as_str().unwrap().to_owned())
                    .collect();

                let index = yaml["index"].as_i64().unwrap() as usize;

                let epoch = yaml["epoch"].as_i64().unwrap() as usize;
                Configure {
                    peer_cnt,
                    peer,
                    index,
                    epoch,
                }
            }
            Err(e) => {
                panic!("Scan yaml file error on {}", e);
            }
        }
    }
}
