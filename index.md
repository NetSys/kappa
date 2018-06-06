---
layout: default
---
Serverless computing (e.g., [AWS Lambda](https://aws.amazon.com/lambda/))
was initially designed for event-driven applications, where each event handler
is guaranteed to complete within a limited time duration.

**{{ site.name }}** aims to enable **general purpose, parallel computation** on
serverless platforms.  To do this, Kappa provides:
- a continuation-based checkpointing mechanism that allows long-running
  computations on time-bounded lambda functions; and,
- a message-passing concurrency API for easily expressing parallelism and
  exploiting the elasticity of serverless platforms.

If you have any questions or feedback, visit the [Support](support/) page or
email us at <span id="email"></span>.

<div id="cucumber">
    <img src="/assets/img/cucumber.svg" class="cucumber">
    <img src="/assets/img/cucumber.svg" class="cucumber">
    <img src="/assets/img/cucumber.svg" class="cucumber">
    <img src="/assets/img/cucumber.svg" class="cucumber">
    <img src="/assets/img/cucumber.svg" class="cucumber">
</div>
<div class="large_banner_wrap">
  <a href="/quick-start" class="large_banner">
    Start using {{ site.name }}
    <i class="fa fa-caret-right"></i><i class="fa fa-caret-right"></i>
  </a>
</div>
