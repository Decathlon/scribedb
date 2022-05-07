# scribedb

![build workflow](https://github.com/decathlon/scribedb/actions/workflows/build.yaml/badge.svg?branch=main)

The tool aims to compare data at schema level between two databases.

For instance, if we compare two dataset with a difference on a single line, we may end up with a result like:

```text
1/3 NOK tgt hash:(6E12FA362B03456CC7601ABEBD454F35) (in 4532.164ms) 40%
src:(50, 60, 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')
tgt:(50, 60, 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNO')
2/3 OK src hash:(5D1FE7284E48A7F751672C7096F3FE98) (in 163.449ms) 79%
Dataset are different
```


Today, postgresql and oracle databases are supported. 

Global Concept [Scribedb in gdoc](https://docs.google.com/presentation/d/1fm95I4YT40y5ZUj8Yaqxk-MaZO0ILIwpwGKuuNAk3JY/edit?usp=sharing)

## Getting Started

You can check the [example](example.md).

## Contributing

New features are always welcome (but first, you should open an issue to discuss new idea)   
Please read [CONTRIBUTING.md](CONTRIBUTING.md) and our [code of conduct](CODE_OF_CONDUCT.md), to check the process for submitting.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/dktunited/scribedb/tags).

## Authors

* **Pierre-Marie Petit** - *Initial work*

See also the list of [contributors](CONTRIBUTORS.md) who participated in this project.

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration

## License

> Copyright 2019-2022 Decathlon.
> 
> Licensed under the Apache License, Version 2.0 (the "License");
> you may not use this file except in compliance with the License.
> You may obtain a copy of the License at
> 
>    http://www.apache.org/licenses/LICENSE-2.0
> 
> Unless required by applicable law or agreed to in writing, software
> distributed under the License is distributed on an "AS IS" BASIS,
> WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
> See the License for the specific language governing permissions and
> limitations under the License.

[Full license](LICENSE)