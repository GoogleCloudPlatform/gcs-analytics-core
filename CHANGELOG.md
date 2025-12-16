# Changelog

## [1.3.0](https://github.com/GoogleCloudPlatform/gcs-analytics-core/compare/v1.2.1...v1.3.0) (2025-12-16)


### Features

* add grpc and direct path support ([#205](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/205)) ([66d1546](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/66d15469a349d4c1f8ce02ca72353dfdd26d407f))


### Bug Fixes

* **deps:** update all non-major dependencies ([#178](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/178)) ([8c39eb5](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/8c39eb5f521d77c48bc2f39378a1de1c87247c78))
* update vectored IO implementation to send bounded request ([#209](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/209)) ([0cf53d7](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/0cf53d7336e191d7bfb4ccc9ebac5b70de98945e))

## [1.2.1](https://github.com/GoogleCloudPlatform/gcs-analytics-core/compare/v1.2.0...v1.2.1) (2025-12-01)


### Bug Fixes

* configuration flags names for GcsReadOptions ([#195](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/195)) ([9aab5ce](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/9aab5ce32816243432bd66e6658d28c8d11e87cb))

## [1.2.0](https://github.com/GoogleCloudPlatform/gcs-analytics-core/compare/v1.1.2...v1.2.0) (2025-11-29)


### Features

* Avoid unnecessary metadata API calls ([#190](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/190)) ([94ae0de](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/94ae0dece5b0a39780e418e03c3fea939f0786c8))


### Bug Fixes

* **deps:** update all non-major dependencies ([#161](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/161)) ([d97a66d](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/d97a66d5e1dfc3118971eda84a7d35fde7686289))
* set chunk-size configuration in read channel. ([#192](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/192)) ([d78fa18](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/d78fa180b6d6d1c884dc2cd3b9024946541eedb0))

## [1.1.2](https://github.com/GoogleCloudPlatform/gcs-analytics-core/compare/v1.1.1...v1.1.2) (2025-10-15)


### Bug Fixes

* **deps:** update all non-major dependencies ([#128](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/128)) ([ffe980b](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/ffe980bbb52d22febad52c176484ea9c18e2a062))
* EOF issue when client requests read with larger buffer than filesize for cached small objects ([3b94dbc](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/3b94dbc1cde466bac5a5a110244afc48e78d872e))
* fix EOF error when readBuffer size is more than fileSize in case of small object cache ([568009f](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/568009f0e7055b23543130487216c5e067882a83))

## [1.1.1](https://github.com/GoogleCloudPlatform/gcs-analytics-core/compare/v1.1.0...v1.1.1) (2025-10-14)


### Bug Fixes

* make configuration flags naming consistent ([#156](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/156)) ([14f88e0](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/14f88e02074413c54c2f77609c92335a03fd5e84))
* Small object cache ([#151](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/151)) ([6dcabad](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/6dcabad2f4c29c50ebd9824907cb545924ea6bbf))

## [1.1.0](https://github.com/GoogleCloudPlatform/gcs-analytics-core/compare/v1.0.0...v1.1.0) (2025-10-08)


### Features

* Expose threaded vectored read in GcsInputStream ([#110](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/110)) ([87851f9](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/87851f9cab0714bb6f713f95355273f02f59026b))
* Implement footer prefetch ([#97](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/97)) ([abc7e9f](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/abc7e9fbdad43c7fc4dfad8f4862ede2aa35c16b))
* Implement Small object caching ([3619d01](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/3619d01d04136b19a560ba383ae170d827976ea7))
* Implement Small object caching ([8fbf4bd](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/8fbf4bd7f614882331212590f0956ba69ba9f974))


### Bug Fixes

* Add support to close fileSystem ([#109](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/109)) ([bac065a](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/bac065a006214ef0d5de5788b92a300db3c1a974))
* **deps:** update all non-major dependencies ([#112](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/112)) ([e106e0f](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/e106e0fcac55266168369b9241d93d85cd51f93b))
* **deps:** update dependency com.google.cloud:google-cloud-storage-bom to v2.57.0 ([#60](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/60)) ([b230e35](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/b230e356832386461d8477f24295644e204dd49b))
* **deps:** update dependency com.google.cloud:libraries-bom to v26.68.0 ([#90](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/90)) ([0e1a300](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/0e1a300d45578f51e3db3eb7030bba7dbb22d4a7))
* Making threadPoolSupplier final, to force initialization before use. ([#98](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/98)) ([2755564](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/2755564403844ca90fc579a93d088ccc2e24a63e))

## 1.0.0 (2025-09-15)


### Features

* initial project setup with signed artifact publishing ([#72](https://github.com/GoogleCloudPlatform/gcs-analytics-core/issues/72)) ([6e3f5e8](https://github.com/GoogleCloudPlatform/gcs-analytics-core/commit/6e3f5e89faec9cfa5242a08c2254ed8a93cf7c1b))
