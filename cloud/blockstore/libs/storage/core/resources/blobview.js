$(function () {
    $('.view').each(function () {
        $('#actions', this).hide();
        $(this).on('mouseleave', function () { $('#actions', this).hide() });
        $(this).on('mouseenter', function () { $('#actions', this).show() });
    })
})
