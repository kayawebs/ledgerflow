//! Who may see what.
//!
//! The public pages exist to bring people in; the analysis that takes real work
//! to produce — a creator's track record, who is buying a launch early, how
//! likely it is to graduate — is core content, shown in full to a reader who
//! has connected a wallet. A paid tier sits above that, so pages ask for a
//! [`Tier`] rather than testing for a wallet directly and every gate moves at
//! once when paid access arrives.

use super::{PageContext, escape_html};

/// What a piece of content requires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// Free to everyone, including crawlers.
    Public,
    /// Requires a connected wallet.
    Connected,
    /// Reserved for a paying reader.
    Paid,
}

/// What the current reader has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    Anonymous,
    Connected,
    Paid,
}

impl Access {
    /// Derives access from the session. Paid access is not sold yet, so a
    /// connected wallet is the highest level a reader can currently reach.
    pub fn of(wallet: Option<&str>) -> Self {
        match wallet {
            Some(_) => Self::Connected,
            None => Self::Anonymous,
        }
    }

    pub fn allows(self, tier: Tier) -> bool {
        match tier {
            Tier::Public => true,
            Tier::Connected => matches!(self, Self::Connected | Self::Paid),
            Tier::Paid => matches!(self, Self::Paid),
        }
    }
}

/// Renders `full` when the reader qualifies, and otherwise the teaser followed
/// by the invitation to connect. The teaser is deliberately a real piece of the
/// answer: enough to know the page holds something, never the answer itself.
pub fn gate(
    context: &PageContext<'_>,
    access: Access,
    tier: Tier,
    teaser: &str,
    full: &str,
) -> String {
    if access.allows(tier) {
        return full.to_string();
    }
    format!("{teaser}{}", invitation(context, tier))
}

/// The call to action shown in place of gated content.
pub fn invitation(context: &PageContext<'_>, tier: Tier) -> String {
    let l = context.locale;
    let (headline, body) = match tier {
        Tier::Paid => (
            l.text("Included with a subscription", "订阅内容"),
            l.text(
                "This analysis is part of the paid tier.",
                "该分析属于付费内容。",
            ),
        ),
        _ => (
            l.text("Connect a wallet to see the rest", "连接钱包查看完整内容"),
            l.text(
                "Creator track records, early-buyer quality and cluster warnings are computed from every trade on the chain. Connecting only proves the address is yours — it moves no funds and costs no gas.",
                "创建者历史、早期买家质量与集群预警，均由链上全部成交计算得出。连接只用于证明地址属于你，不会动用任何资金，也不消耗 gas。",
            ),
        ),
    };
    format!(
        r#"<article class="card locked"><div class="locked-body"><strong>{headline}</strong><p>{body}</p><button class="wallet" type="button" data-wallet-connect data-stay>{action}</button></div></article>"#,
        headline = escape_html(headline),
        body = escape_html(body),
        action = escape_html(l.text("Connect wallet", "连接钱包")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_web::{Locale, PageQuery};

    fn context(query: &PageQuery) -> PageContext<'_> {
        PageContext::new(query)
    }

    #[test]
    fn connecting_unlocks_connected_content_but_not_paid() {
        let anonymous = Access::of(None);
        let connected = Access::of(Some("0xabc"));
        assert!(anonymous.allows(Tier::Public));
        assert!(!anonymous.allows(Tier::Connected));
        assert!(connected.allows(Tier::Connected));
        assert!(!connected.allows(Tier::Paid));
        assert!(Access::Paid.allows(Tier::Connected));
    }

    #[test]
    fn a_gate_shows_the_teaser_and_an_invitation_but_never_the_content() {
        let query = PageQuery::default();
        let context = context(&query);
        let rendered = gate(
            &context,
            Access::Anonymous,
            Tier::Connected,
            "<p>teaser</p>",
            "<p>secret</p>",
        );
        assert!(rendered.contains("teaser"));
        assert!(!rendered.contains("secret"));
        assert!(rendered.contains("data-wallet-connect"));
    }

    #[test]
    fn a_connected_reader_sees_the_content_without_the_invitation() {
        let query = PageQuery::default();
        let context = context(&query);
        let rendered = gate(
            &context,
            Access::Connected,
            Tier::Connected,
            "<p>teaser</p>",
            "<p>secret</p>",
        );
        assert!(rendered.contains("secret"));
        assert!(!rendered.contains("data-wallet-connect"));
    }

    #[test]
    fn the_invitation_is_translated() {
        let zh = PageQuery {
            lang: Some("zh".to_owned()),
            ..PageQuery::default()
        };
        let context = context(&zh);
        assert_eq!(context.locale, Locale::Zh);
        assert!(invitation(&context, Tier::Connected).contains("连接钱包"));
    }
}
