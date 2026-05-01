# Changelog

## 0.1.0 (2026-05-01)


### Features

* **gdelt:** wire events+mentions bulk ingester, add startup cooldown to doc/tv ([acb4d22](https://github.com/Weston-Tyler/osint_tool/commit/acb4d229357b05a4af051ffcd5f2b52b98584ee6))
* **gfw:** add rate limiting, retry/backoff, --max-pages cap ([9b13df5](https://github.com/Weston-Tyler/osint_tool/commit/9b13df51129f33df273ed66af990300e42f59c2c))
* **gfw:** full-coverage ingester + fix causal schema syntax error ([d70044e](https://github.com/Weston-Tyler/osint_tool/commit/d70044e912e5a5489e7a5d2bdba9b8e25138c518))
* **gfw:** vessel identity enrichment worker ([97ea219](https://github.com/Weston-Tyler/osint_tool/commit/97ea2191f8aa6735ca87cea577e58fef3c1bcdc8))
* **graph-processor:** consume GFW v3 topics → typed Memgraph nodes ([ecf6c05](https://github.com/Weston-Tyler/osint_tool/commit/ecf6c05fcd0357f796ca59ef458fee05283daf56))


### Bug Fixes

* **api:** add missing kafka-python dependency ([bb0aebb](https://github.com/Weston-Tyler/osint_tool/commit/bb0aebb75c5006fa20be64de5a5ab4a4b516602a))
* **api:** api package layout + ownership-api port mapping ([385c09c](https://github.com/Weston-Tyler/osint_tool/commit/385c09c27d0d75d46961d17ac24ef6fb6468df2d))
* **api:** inline hours literal in uas detections Cypher ([c3d80f3](https://github.com/Weston-Tyler/osint_tool/commit/c3d80f35832588f55a6e871549a3558c81fab8fd))
* **api:** memgraph duration() uses singular 'hour' key not 'hours' ([e5b9d4b](https://github.com/Weston-Tyler/osint_tool/commit/e5b9d4b02819c3132ba9db90717b980dae0884bb))
* **compose:** move worker-gdelt to batch profile (one-shot ingester) ([5a4ffe6](https://github.com/Weston-Tyler/osint_tool/commit/5a4ffe61b47863c54ce9170568c74550ac8377ac))
* **compose:** tune postgres/memgraph/elasticsearch for Codespaces ([cc7f077](https://github.com/Weston-Tyler/osint_tool/commit/cc7f07784cf2dc23664df75d886ae131ae8af6c4))
* **docker:** ownership-api build context — use parent dir to reach shell_detector ([2c0b7bc](https://github.com/Weston-Tyler/osint_tool/commit/2c0b7bcf58e9236d2ce152b5d6ef0ff45a2cddaf))
* **docker:** proper Dockerfiles for property/shell/tco/entity services ([c3b96ec](https://github.com/Weston-Tyler/osint_tool/commit/c3b96ecf02c768d12caa22c0a307633b699efbf4))
* **gdelt:** bump rate limit to 5.5s, disable defunct GEO 2.0 endpoint ([ad06dac](https://github.com/Weston-Tyler/osint_tool/commit/ad06daca63ce7a7003f4459c765c9c345965af91))
* **gdelt:** rate limit + retry/backoff + daemon loop on doc/geo/tv ([16e2f24](https://github.com/Weston-Tyler/osint_tool/commit/16e2f24816460966d3f96f424c6e64961fde808c))
* **gfw-enrich:** normalize snake_case keys, handle nameless vessels ([d7fdae7](https://github.com/Weston-Tyler/osint_tool/commit/d7fdae7e37ffa41955e97f19e33143554515ba4c))
* **graph-processor:** set MEMGRAPH_HOST/PORT env vars ([375b8be](https://github.com/Weston-Tyler/osint_tool/commit/375b8be819e394713533ac19530745edcd67a6bc))
* **memgraph:** bump start_period to 180s for mage module load ([5075e57](https://github.com/Weston-Tyler/osint_tool/commit/5075e57c5bbba26deacdaaa857eb5ec0b133dbd1))
* **memgraph:** healthcheck uses bash /dev/tcp (nc not in mage image) ([cd13796](https://github.com/Weston-Tyler/osint_tool/commit/cd1379601de4aabce1f8381eb5aa4b47d23357a5))
* **memgraph:** pin to real tag 3.9.0 (1.22-memgraph-2.18 does not exist) ([be7e45b](https://github.com/Weston-Tyler/osint_tool/commit/be7e45be288b5bc878af3870c6091c2e70f6de41))
* **memgraph:** use stable mage tag, proper --memory-limit flag ([deda179](https://github.com/Weston-Tyler/osint_tool/commit/deda1798a99419571ebb58db3218084e3c162d86))
* **ownership-api:** add MEMGRAPH_URI env var ([f98f936](https://github.com/Weston-Tyler/osint_tool/commit/f98f93603de643f8e0d1d13c5ca12e3e21e4506f))
* **postgres:** build custom image with AGE + PostGIS ([576a0eb](https://github.com/Weston-Tyler/osint_tool/commit/576a0eb1af0a8b573888001d6b900215da6e308c))
* **postgres:** create vector + postgis extensions before tables use them ([e10654b](https://github.com/Weston-Tyler/osint_tool/commit/e10654ba07c6676505f7f608202e08e8d7817830))
* **redpanda:** keep smp=4 (cannot decrease without wiping volume) ([969c917](https://github.com/Weston-Tyler/osint_tool/commit/969c917e0c7762ed09df89979bdad907a96530a7))
* **redpanda:** raise partition limit + add gfw-topics target + ATTRIBUTIONS ([c18d6e9](https://github.com/Weston-Tyler/osint_tool/commit/c18d6e9cd93ea96474fdc9a48086b403c27a67ce))
* **sanctions:** unify on OpenSanctions, retire dead OFAC URL ([6f2ca60](https://github.com/Weston-Tyler/osint_tool/commit/6f2ca6064d04ace2d7838a63d4c046d440771a6e))
* **topics:** run rpk inside mda-redpanda container instead of host ([63c3517](https://github.com/Weston-Tyler/osint_tool/commit/63c3517ef45a8ee775f6bd62112635e23daa7beb))
* update Memgraph image — platform:2.14.0 removed upstream ([7d44654](https://github.com/Weston-Tyler/osint_tool/commit/7d44654b41b67e7a5501232dae3510475581a3eb))
* **verify+setup:** script bugs, pgvector, expansion schemas, expanded setup ([9ebf46f](https://github.com/Weston-Tyler/osint_tool/commit/9ebf46f591280a5e72afdbe4267320e2b858b392))
* **verify:** leftover BOOTSTRAP ref, GeoServer http-code check, SHOW STREAMS ([430680a](https://github.com/Weston-Tyler/osint_tool/commit/430680a26d9a34a8f17b4483ac65306b5b0cd711))
* **verify:** OFAC moved to sanctionslistservice, ReliefWeb v2 needs appname ([71a7617](https://github.com/Weston-Tyler/osint_tool/commit/71a7617144ee19afbaa8be860d63e2d77629ae51))
* **verify:** reliefweb v1 decommissioned, use v2 ([6ba1772](https://github.com/Weston-Tyler/osint_tool/commit/6ba17727363bc47750f2d89ef46b475e5baaa3de))
* **verify:** remove dollar sign from coingecko test (bash eval expansion) ([8b439ea](https://github.com/Weston-Tyler/osint_tool/commit/8b439eadcd7b06790a0a7c2562711c666143a80a))
* **verify:** use registered ReliefWeb appname mda-osint-verify-7k3p ([644b4ed](https://github.com/Weston-Tyler/osint_tool/commit/644b4ed88d80ce269aa33d5ac3e8fcda3edbd837))
* **worker-ais:** add gqlalchemy dependency for enrichment worker ([727f7b1](https://github.com/Weston-Tyler/osint_tool/commit/727f7b1258ee869de71a197ec4a1fc823e813942))
* **workers:** batch profile for one-shot ingesters + gdelt parser fix ([d2ee0b6](https://github.com/Weston-Tyler/osint_tool/commit/d2ee0b6805d473e6face893d75fc635ac27e6099))


### Documentation

* add Counter-UAS expansion 6 plan + stack compatibility notes ([a1c6aa6](https://github.com/Weston-Tyler/osint_tool/commit/a1c6aa6129af254d402856faf82bfa383bd8c889))
* add MDA OSINT intelligence query collection (10 investigations) ([0696a3b](https://github.com/Weston-Tyler/osint_tool/commit/0696a3b10f519236523f4262513c61911fc45717))
