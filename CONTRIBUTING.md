# Contributing to this project

If you are on this page it means you are almost ready to contribute proposing changes, fixing issues or anything else.
So, **thanks for your time** !! :tada::+1:
Any idea to help improving the Decathlon platform is really welcome, because it is **your platform** too !


<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Contributing to this project](#contributing-to-this-project)
  - [Code of conduct](#code-of-conduct)
  - [What are you talking about? Pull Request? Merge? Push?](#what-are-you-talking-about-pull-request-merge-push)
  - [I don't want to read this whole thing I just have a question!!!](#i-dont-want-to-read-this-whole-thing-i-just-have-a-question)
  - [How Can I contribute?](#how-can-i-contribute)
    - [Reporting Bug](#reporting-bug)
    - [Suggesting enhancement](#suggesting-enhancement)
    - [Code Contribution](#code-contribution)
      - [Commit and Push on your branch](#commit-and-push-on-your-branch)
    - [Pull Request guidelines](#pull-request-guidelines)
    - [Contribution acceptance criteria](#contribution-acceptance-criteria)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Code of conduct
This project and everyone participating in it is governed by this [code of conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## What are you talking about? Pull Request? Merge? Push?
If you are not familiar with Git and GitHub terms you can check a complete [glossary](https://help.github.com/articles/github-glossary/) on the GitHub website.
## I don't want to read this whole thing I just have a question!!!

> **NOTE:** Please don't file an issue to simply ask a question. It will be faster if you will use any of the following channel.

 * For anything related to the `project` RoadMap, opened issues, etc. you can directly check our [Open Dashboard](https://github.com/orgs/dktunited/projects/2) or subscribing to our internal [Google+ community](https://plus.google.com/communities/106878661287285854741)
 * If you want to ask a question, support to the team you can send a [mail message](mailto:myteam@mydomain.net)
 * If you prefer the live chatting join us on our [Slack Channel](https://decathlon-commerce.slack.com/messages/CBZBP0821) (into the Decathlon Commerce Slack).

## How Can I contribute?

### Reporting Bug
The first way to contribute to a project is simply reporting a Bug. If you find anything which is not working well or as expected you can open an Issue the [my-project](https://github.com/dktunited/scribedb/issues) repository.

Before to open the issue please check if there is one similar already opened on our [project board](https://github.com/orgs/dktunited/projects/2) or the `project` repository. It will save us hours of work and it will allow us to answer you quickly with the desired hotfix.

> **NOTE:** if looking for existing issues you will find the same problem, or similar, in **closed** state, please refer to this issue (with it's number) when you are opening your one. It is maybe a regression we didn't see. In this way you will help to go faster and to find a definitive solution to the recurrent problem.

When you are opening an issue, please be sure to report as much information as you can to allow us to replicate the problem and faster find the solution. To help you on this task, the `project` repository is providing Issues Templates:

* Bug Report
* Feature Request
* Custom

![Issues](images/issues.png)

If you chose the *Bug Report* template there is some guideline helping you to fill up the issue. You don't have to provide all the information you will find on the template but, try to be your best.

### Suggesting enhancement
If you think we can to better with the `project` product and you have some good ideas, you can file an issue selecting the *Feature Request* template.
Even in this case please try to be as much clear as possible. The faster we can understand what you want or what you are proposing, the faster the implementation will be.

For any request suggestion, the `project` product Owner and team is keeping the responsibility to take the final decision: we have a road map and a vision for the product that **must remains generic**.
If the requests are custom specific we will try to find a different way to help you, or, you can implement what you need in a PaymentServiceProvider plugin.

Any issue can be voted by the communities and `project` follower and, even if we are thinking it is not a good idea to implement it, we can surely listen something which is appreciated by the many.

### Code Contribution
If you are a dev and you want to directly fix a problem or implement a new feature... you are the best one ! :clap::clap:
To propose any change you have to submit us a [PullRequest](https://help.github.com/articles/about-pull-requests/)

The workflow we are using the one-pay project is:
1. Fork the `project` repository (as you don't have a direct write access to the main one. For the moment :wink:
2. Create your code, `Commit` and `Push the code` on your forked repo
3. Create a GitHub `Pull Request` to our **develop** branch (which is the main one for the coming version).

We will take the time to review your code, make some comments or asking information if needed. But, as you took time to help us, we will take in serious consideration what you are proposing.
To quickly have your code available on production, please take care and read our [Contribution acceptance criteria](#contribution-acceptance-criteria)

#### Commit and Push on your branch
```bash
git add <files>
git commit -m "A commit message"
git push origin <my-branch-name> 
```

### Pull Request guidelines
When you open your pull request provide as much information as possible. You use the templates described into the [reporting bug](#reporting-bug) section.
* For an issue, describe what you are fixing with your pull request and how you had found the defect.
* If you are proposing an enhancement, describe what you are adding to the code (new function, performance enhancement, documentation update, changing an existing function, ...).

### Contribution acceptance criteria
We love maintenable software and we are happy when some else than us is able to take the code, **understand it** and be able to change it.
To reach this goal we fixed some rule in our team and we would love to go ahead in this way, even with the external contribution:
1. Be sure your code compile: no syntax error, no missing library, ...
2. Be sure to write a JUnit test covering almost all the code you wrote. 100% of code coverage is usless, we just need all the tests allowing to understand the possible cases about the input and excpected output.
3. Add comments on the code if you want to explain better what is happening in the code.
Please not stuffs like:
```java
//The product Id
String productId;
```
4. Add documentation for any API, if needed, or functional explaining what changed/added with your code.
5. After you proposed the PullRequest, our CI/CD platform will execute all the steps defined in our [delivery chain](documentation/DELIVERY-CHAIN.md). If you will receive any mail or find any automatic comment on the Pull Request you opened, it means there is something which is not respecting the project defined code style or your broke any previously created test.

If you respect all these rules you will help us saving time and we will be able to check your pull request faster.