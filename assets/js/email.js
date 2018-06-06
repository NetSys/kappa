var username = "zhangwen";
var hostname = "cs.berkeley.edu";
var linktext = username + "@" + hostname;
document.getElementById('email').innerHTML = '<a href="mailto:' + linktext + '">' + linktext + '</a>';
