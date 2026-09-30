#[test]
fn nzb_subject_suffix_should_not_become_the_media_extension() {
    let f = nzbd_nzb::ParsedFile {
        subject: "[PRiVATE]-[WtFnZb]-[Show.S01E01.mkv]-[2/11] - yEnc 4194484837 (1/5852)".into(),
        ..Default::default()
    };
    assert_eq!(f.filename_hint(), "Show.S01E01.mkv");
}
