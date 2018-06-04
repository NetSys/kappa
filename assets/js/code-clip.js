var allCode = $( "code" );

var shellPrompt = /[^$]*\$ (.*)/
counter = 0;
allCode.each(function(u) {
    var toProc = $(this).text();
    var lines = $(this).text().split('\n');
    // include copy button if shell prompt or great-grandparent class is copy.
    var override = $(this).parent().parent().parent();
    var copy = override.hasClass("copy");
    var promptExists = false;
    var res = [];
    for(var i = 0; i < lines.length; i++) {
        var match = lines[i].match(shellPrompt);
        if (match != null) {
            res.push(match[1]);
            promptExists = true;
        } else if (copy) {
            res.push(lines[i]);
        }
    }

    if (override.hasClass("no_copy") || (!promptExists && !copy)) {
        return;
    }

    counter += 1;
    var currentId = "block" + (counter + 1);
    var invisible = $("<code>", {
        id: currentId,
        class: "invisible",
    });
    invisible.text(res.join('\n'));

    var button = $("<button>", {
        "data-id": '#'+currentId,
        class: "btn",
    });
    button.html('<i class="fa fa-clipboard"></i>')
    $(this).parent().parent().append(invisible);
    $(this).parent().parent().append(button);
});

new ClipboardJS(".btn", {
    text: function(trigger) {
        var id = trigger.getAttribute('data-id');
        return $(id).text();
    }
});
//new ClipboardJS('.btn');
