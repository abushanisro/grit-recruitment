import React from "react";
import { TermsAndCondition } from "@/types/termsAndCondition";
import { Anchor, Box, List, ScrollArea, Text } from "@mantine/core";
import { useMediaQuery } from "@mantine/hooks";
import classes from "styles/termsAndCondition.module.css";

const TermsAndConditionDataPage = () => {
  const isMobile = useMediaQuery("(max-width: 426px)");
  const isSmallerMobile = useMediaQuery("(max-width: 338px)");
  const CustomListItem = ({ label }: { label: string }) => {
    return (
      <List.Item
        style={{ textAlign: "justify" }}
        mt={{ base: 14, lg: 30, xl: 30 }}
        fz={{ base: 14, xs: 16, sm: 18, md: 18, lg: 18, xl: 18 }}
        fw={400}
        px={20}
      >
        {label}
      </List.Item>
    );
  };
  const section1Data: TermsAndCondition[] = [
    {
      heading: "Welcome to Sales Jobverse.",
      content: [
        `
         This website is operated by Sales Jobverse. Throughout the site, the terms “we”, “us” and “our' refer to Sales Jobverse. Sales Jobverse offers this website, including all information, tools and services available from this site to you, the user, conditioned upon your acceptance of all terms, conditions, policies and notices stated here.`,

        `By visiting our site and/ or purchasing something from us, you engage in our “Service” and agree to be bound by the following terms and conditions (“Terms of Service”, “Terms”), including those additional terms and conditions and policies referenced herein and/or available by hyperlink. These Terms of Service apply to all users of the site, including without limitation users who are browsers, vendors, customers, merchants, and/ or contributors of content
.
`,
        `Please read these Terms of Service carefully before accessing or using our website. By accessing or using any part of the site, you agree to be bound by these Terms of Service. If you do not agree to all the terms and conditions of this agreement, then you may not access the website or use any services. If these Terms of Service are considered an offer, acceptance is expressly limited to these Terms of Service.

`,
        `If you are accessing or using our Services on behalf of a company or other legal entity, you represent and warrant that you are an authorised representative of that company or entity and that your access or use is authorised by such entity. You further represent and warrant that such company or entity accepts these Terms.


`,
        `Any new features or tools which are added to the site shall also be subject to the Terms of Service. You can review the most current version of the Terms of Service at any time on this page. We reserve the right to update, change or replace any part of these Terms of Service by posting updates and/or changes to our website. It is your responsibility to check this page periodically for changes. Your continued use of or access to the website following the posting of any changes constitutes acceptance of those changes.

`,
      ],

      doesHeadingHaveBorder: true,
    },
    {
      heading: "SECTION 1 - TERMS",
      content: [
        `By agreeing to these Terms of Service, you represent that you are at least the age of majority in your state or province of residence, or that you are the age of majority in your state or province of residence and you have given us your consent to allow any of your minor dependents to use this site.
`,
        `You may not use our products for any illegal or unauthorised purpose nor may you, in the use of the Service, violate any laws in your jurisdiction (including but not limited to copyright laws).

`,
        `You must not transmit any worms or viruses or any code of a destructive nature.

`,
        `A breach or violation of any of the Terms will result in an immediate termination of your Services.

`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 2 - GENERAL CONDITIONS",
      content: [
        `We reserve the right to refuse service to anyone for any reason at any time.
`,
        `You understand that your content (excluding payment information), may be transferred unencrypted and involve (a) transmissions over various networks; and (b) changes to conform and adapt to the technical requirements of connecting networks or devices.

`,
        `You agree not to reproduce, duplicate, copy, sell, resell or exploit any portion of the Service, use of the Service, or access to the Service or any contact on the website through which the service is provided, without express written permission by us.

`,
        `
        The headings used in this agreement are included for convenience only and will not limit or otherwise affect these Terms.

`,
      ],
      doesHeadingHaveBorder: false,
    },
  ];

  const section2Data: TermsAndCondition[] = [
    {
      heading:
        "SECTION 4 - ACCURACY, COMPLETENESS AND TIMELINESS OF INFORMATION",
      content: [
        `We are not responsible if information made available on this site is not accurate, complete or current. The material on this site is provided for general information only and should not be relied upon or used as the sole basis for making decisions without consulting primary, more accurate, more complete or more timely sources of information. Any reliance on the material on this site is at your own risk.
`,
        `This site may contain certain historical information. Historical information, necessarily, is not current and is provided for your reference only. We reserve the right to modify the contents of this site at any time, but we have no obligation to update any information on our site. You agree that it is your responsibility to monitor changes to our site.
`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 5 - MODIFICATIONS TO THE SERVICE AND PRICES",
      content: [
        `Prices for our products are subject to change without notice.
`,
        `We reserve the right at any time to modify or discontinue the Service (or any part or content thereof) without notice at any time.
`,
        `We shall not be liable to you or to any third party for any modification, price change, suspension or discontinuance of the Service.
`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 6 - SERVICES",
      content: [
        `We reserve the right but are not obligated to limit services to any person, geographic region or jurisdiction. We may exercise this right on a case-by-case basis. We reserve the right to limit the quantities of any services that we offer. All descriptions of pricing are subject to change at any time without notice, at our sole discretion. We reserve the right to discontinue any service at any time. Any offer for any service made on this site is void where prohibited.
`,
        `We do not warrant that the quality of any services obtained by you will meet your expectations.

`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "Section 7 - OPTIONAL TOOLS & RESOURCES",
      content: [
        `We may provide you with access to third-party tools and resources over which we neither monitor nor have any control nor input. You acknowledge and agree that we provide access to such tools and resources ”as is” and “as available” without any warranties, representations or conditions of any kind and without any endorsement. We shall have no liability whatsoever arising from or relating to your use of optional third-party tools and resources.
`,
        `Any use by you of optional tools and resources offered through the site is entirely at your own risk and discretion and you should ensure that you are familiar with and approve of the terms on which tools and resources are provided by the relevant third-party provider(s).
`,
        `We may also, in the future, offer new services and/or features through the website (including, the release of new tools and resources). Such new features and/or services shall also be subject to these Terms of Service.
`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 8 - THIRD PARTY LINKS",
      content: [
        `‍Our services may contain links to other sites that are not operated or owned by us. Some of their links could be affiliated links. If you click a third-party link, you will be directed to their site. We are not responsible for examining or evaluating the content or accuracy and we do not warrant and will not have any liability or responsibility for any third-party materials or websites, or for any other materials, products, or services of third parties. We are not liable for any harm or damages related to the purchase or use of goods, services, resources, content, or any other transactions made in connection with any third-party websites. Please review carefully the third-party's policies and practices and make sure you understand them before you engage in any transaction. Complaints, claims, concerns, or questions regarding third-party products should be directed to the third party. We strongly advise you to go over the privacy policy of the site you visit.
`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 9 - USER COMMENTS, FEEDBACK AND OTHER SUBMISSIONS",
      content: [
        `If, at our request, you send certain specific submissions (for example contest entries) or without a request from us you send creative ideas, suggestions, proposals, plans, or other materials, whether online, by email, by postal mail, or otherwise (collectively, 'comments'), you agree that we may, at any time, without restriction, edit, copy, publish, distribute, translate and otherwise use in any medium any comments that you forward to us. We are and shall be under no obligation (1) to maintain any comments in confidence; (2) to pay compensation for any comments; or (3) to respond to any comments.
`,
        `We may, but have no obligation to, monitor, edit or remove content that we determine in our sole discretion are unlawful, offensive, threatening, libellous, defamatory, pornographic, obscene or otherwise objectionable or violates any party’s intellectual property or these Terms of Service.

`,
        `You agree that your comments will not violate any right of any third party, including copyright, trademark, privacy, personality or another personal or proprietary right. You further agree that your comments will not contain libellous or otherwise unlawful, abusive or obscene material, or contain any computer virus or other malware that could in any way affect the operation of the Service or any related website. You may not use a false e-mail address, pretend to be someone other than yourself, or otherwise mislead us or third parties as to the origin of any comments. You are solely responsible for any comments you make and their accuracy. We take no responsibility and assume no liability for any comments posted by you or any third party
`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 10 - PERSONAL INFORMATION",
      content: [
        `Your submission of personal information through the store is governed by our Privacy Policy.

`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 11 - ERRORS, INACCURACIES AND OMISSIONS",
      content: [
        `Occasionally there may be information on our site or in the Service that contains typographical errors, inaccuracies or omissions that may relate to descriptions, pricing, promotions, offers, product shipping charges, transit times and availability. We reserve the right to correct any errors, inaccuracies or omissions, and to change or update information or cancel orders if any information in the Service or on any related website is inaccurate at any time without prior notice (including after you have submitted your order).

`,
        `We undertake no obligation to update, amend or clarify information in the Service or on any related website, including without limitation, pricing information, except as required by law. No specified update or refresh date applied in the Service or on any related website should be taken to indicate that all information in the Service or on any related website has been modified or updated.
`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 12 - PROHIBITED USES",
      content: [
        `In addition to other prohibitions as set forth in the Terms of Service, you are prohibited from using the site or its content: (a) for any unlawful purpose; (b) to solicit others to perform or participate in any unlawful acts; (c) to violate any international, federal, provincial or state regulations, rules, laws, or local ordinances; (d) to infringe upon or violate our intellectual property rights or the intellectual property rights of others; (e) to harass, abuse, insult, harm, defame, slander, disparage, intimidate, or discriminate based on gender, sexual orientation, religion, ethnicity, race, age, national origin, or disability; (f) to submit false or misleading information; (g) to upload or transmit viruses or any other type of malicious code that will or may be used in any way that will affect the functionality or operation of the Service or of any related website, other websites, or the Internet; (h) to collect or track the personal information of others; (i) to spam, phish, pharm, pretext, spider, crawl, or scrape; (j) for any obscene or immoral purpose; or (k) to interfere with or circumvent the security features of the Service or any related website, other websites, or the Internet. We reserve the right to terminate your use of the Service or any related website for violating any of the prohibited uses.
`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 13 - DISCLAIMER OF WARRANTIES; LIMITATION OF LIABILITY",
      content: [
        `We do not guarantee, represent or warrant that your use of our service will be uninterrupted, timely, secure or error-free.
`,
        `We do not warrant that the results that may be obtained from the use of the service will be accurate or reliable.
`,
        `You agree that from time to time we may remove the service for indefinite periods of time or cancel the service at any time, without notice to you. You expressly agree that your use of, or inability to use, the service is at your sole risk. The service and all products and services delivered to you through the service are (except as expressly stated by us) provided 'as is' and 'as available for your use, without any representation, warranties or conditions of any kind, either express or implied, including all implied warranties or conditions of merchantability, merchantable quality, fitness for a particular purpose, durability, title, and non-infringement.
`,
        `In no case shall Sales Jobverse, our directors, officers, employees, affiliates, agents, contractors, interns, suppliers, service providers or licensors be liable for any injury, loss, claim, or any direct, indirect, incidental, punitive, special, or consequential damages of any kind, including, without limitation lost profits, lost revenue, lost savings, loss of data, replacement costs, or any similar damages, whether based in contract, tort (including negligence), strict liability or otherwise, arising from your use of any of the service or any products procured using the service, or for any other claim related in any way to your use of the service or any product, including, but not limited to, any errors or omissions in any content, or any loss or damage of any kind incurred as a result of the use of the service or any content (or product) posted, transmitted, or otherwise made available via the service, even if advised of their possibility.

`,
        `
         Because some states or jurisdictions do not allow the exclusion or the limitation of liability for consequential or incidental damages, in such states or jurisdictions, our liability shall be limited to the maximum extent permitted by law.
`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 14 - INDEMNIFICATION",
      content: [
        `You agree to indemnify, defend and hold harmless Sales Jobverse and our parent, subsidiaries, affiliates, partners, officers, directors, agents, contractors, licensors, service providers, subcontractors, suppliers, interns and employees, harmless from any claim or demand, including reasonable attorneys’ fees, made by any third party due to or arising out of your breach of these Terms of Service or the documents they incorporate by reference or your violation of any law or the rights of a third-party.

`,
      ],

      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 15 - SEVERABILITY",
      content: [
        `In the event that any provision of these Terms of Service is determined to be unlawful, void or unenforceable, such provision shall nonetheless be enforceable to the fullest extent permitted by applicable law, and the unenforceable portion shall be deemed to be severed from these Terms of Service, such determination shall not affect the validity and enforceability of any other remaining provisions.

`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 16 - TERMINATION",
      content: [
        `The obligations and liabilities of the parties incurred prior to the termination date shall survive the termination of this agreement for all purposes.
`,
        `These Terms of Service are effective unless and until terminated by either you or us. You may terminate these Terms of Service at any time by notifying us that you no longer wish to use our Services, or when you cease using our site. If in our sole judgement you fail, or we suspect that you have failed, to comply with any term or provision of these Terms of Service, we also may terminate this agreement at any time without notice and you will remain liable for all amounts due up to and including the date of termination; and/or accordingly may deny you access to our Services (or any part thereof).
`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 17 - ENTIRE AGREEMENT",
      content: [
        `The failure of us to exercise or enforce any right or provision of these Terms of Service shall not constitute a waiver of such right or provision.

`,
        `These Terms of Service and any policies or operating rules posted by us on this site or in respect to The Service constitute the entire agreement and understanding between you and us and govern your use of the Service, superseding any prior or contemporaneous agreements, communications and proposals, whether oral or written, between you and us (including, but not limited to, any prior versions of the Terms of Service). Any ambiguities in the interpretation of these Terms of Service shall not be construed against the drafting party.
`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 18 - GOVERNING LAW",
      content: [
        `These Terms of Service and any separate agreements whereby we provide you Services shall be governed by and construed in accordance with the laws of Chennai, Tamil Nadu, India.
`,
      ],
      doesHeadingHaveBorder: false,
    },
    {
      heading: "SECTION 19 - CHANGES TO TERMS OF SERVICE",
      content: [
        `You can review the most current version of the Terms of Service at any time at this page. We reserve the right, at our sole discretion, to update, change or replace any part of these Terms of Service by posting updates and changes to our website. It is your responsibility to check our website periodically for changes. Your continued use of or access to our website or the Service following the posting of any changes to these Terms of Service constitutes acceptance of those changes.
`,
      ],
      doesHeadingHaveBorder: false,
    },
  ];

  return (
    <Box>
      <Box
        bg={"primaryGrey.1"}
        style={{
          borderRadius: 10,
          height: isMobile ? 80 : 120,
          width: "100%",
        }}
      >
        <Text
          pt={isMobile ? 30 : 60}
          fz={{ base: 20, xs: 24, sm: 26, md: 33, lg: 33, xl: 33 }}
          fw={700}
          style={{ textAlign: "center" }}
        >
          Terms & Conditions
        </Text>
      </Box>
      <ScrollArea h={800}>
        <Box
          px={{ base: isSmallerMobile ? 10 : 20, xs: 50, sm: 50, xl: 70 }}
          py={{ base: 20, xs: 50, sm: 50, xl: 60 }}
        >
          {section1Data.map((datas, index) => (
            <Box key={index}>
              <Text
                mt={{ base: 14, lg: 30, xl: 30 }}
                c="#FFF"
                fw={700}
                fz={{ base: 18, xs: 20, sm: 22, md: 24, lg: 24, xl: 24 }}
                style={{
                  textDecoration: datas.doesHeadingHaveBorder
                    ? "underline"
                    : "",
                  textUnderlineOffset: "10px",
                }}
              >
                {datas.heading}
              </Text>

              {datas.content.map((contents, index) => (
                <Box key={index}>
                  <Text
                    style={{ textAlign: "justify" }}
                    mt={{ base: 14, lg: 30, xl: 30 }}
                    fz={{ base: 14, xs: 16, sm: 18, md: 18, lg: 18, xl: 18 }}
                    fw={400}
                  >
                    {contents}
                  </Text>
                </Box>
              ))}
            </Box>
          ))}
          <Box>
            <Text
              mt={{ base: 14, lg: 30, xl: 30 }}
              c="#FFF"
              fw={700}
              fz={{ base: 18, xs: 20, sm: 22, md: 24, lg: 24, xl: 24 }}
            >
              SECTION 3 - JOB POSTINGS AND LISTINGS
            </Text>
            <Text
              style={{ textAlign: "justify" }}
              mt={{ base: 14, lg: 30, xl: 30 }}
              fz={{ base: 14, xs: 16, sm: 18, md: 18, lg: 18, xl: 18 }}
              fw={400}
            >
              Sales Jobverse provides a platform for users to post and search
              for sales related job opportunities in India. We do not guarantee
              the accuracy, authenticity, or availability of the job listings,
              nor do we endorse any particular job posting.
            </Text>
            <Text
              style={{ textAlign: "justify" }}
              mt={{ base: 14, lg: 30, xl: 30 }}
              fz={{ base: 14, xs: 16, sm: 18, md: 18, lg: 18, xl: 18 }}
              fw={400}
            >
              Users are solely responsible for the accuracy and legitimacy of
              the job postings they submit. Sales Jobverse reserves the right to
              remove or modify any job posting that violates these Terms and
              Conditions or is deemed inappropriate.
            </Text>
            <Text
              style={{ textAlign: "justify" }}
              mt={{ base: 14, lg: 30, xl: 30 }}
              fz={{ base: 14, xs: 16, sm: 18, md: 18, lg: 18, xl: 18 }}
              fw={400}
            >
              As a recruiter posting jobs on Sales Jobverse, you confirm that:
            </Text>
            <Box px={20}>
              <List>
                <CustomListItem
                  label={`You have authority to post jobs on behalf of your organisation and authority to share information with Sales Jobverse
                       `}
                />

                <CustomListItem
                  label={` You shall post legitimate and legal job opportunities with
                accurate information`}
                />

                <CustomListItem
                  label={`You will not engage in fraudulent or exploitative activity with
                candidates responding to your job post`}
                />

                <CustomListItem
                  label={` You understand that satisfactory candidate response or hiring
                outcomes are not guaranteed`}
                />
              </List>
            </Box>
            <Text
              style={{ textAlign: "justify" }}
              mt={{ base: 14, lg: 30, xl: 30 }}
              fz={{ base: 14, xs: 16, sm: 18, md: 18, lg: 18, xl: 18 }}
              fw={400}
            >
              As a candidate seeking jobs on Sales Jobverse, you confirm that:
            </Text>
            <List px={20}>
              <CustomListItem
                label={`You are responsible to verify accuracy of employer information
                and conduct your own due diligence to participate in the
                selection process and joining an employer`}
              />
              <CustomListItem
                label={`  You will not misrepresent information to prospective employers,
                or engage in any form of fraudulent or exploitative activity
                with prospective employers.`}
              />
              <CustomListItem
                label={` You understand Sales Jobverse does not guarantee any satisfactory outcome for any jobs posted on our site
               `}
              />
            </List>
          </Box>
          {section2Data.map((datas, index) => (
            <Box key={index}>
              <Text
                mt={{ base: 14, lg: 30, xl: 30 }}
                c="#FFF"
                fw={700}
                fz={{ base: 18, xs: 20, sm: 22, md: 24, lg: 24, xl: 24 }}
                style={{
                  textDecoration: datas.doesHeadingHaveBorder
                    ? "underline"
                    : "",
                  textUnderlineOffset: "10px",
                }}
              >
                {datas.heading}
              </Text>

              {datas.content.map((contents, index) => (
                <Box key={index}>
                  <Text
                    style={{ textAlign: "justify" }}
                    mt={{ base: 14, lg: 30, xl: 30 }}
                    fz={{ base: 14, xs: 16, sm: 18, md: 18, lg: 18, xl: 18 }}
                    fw={400}
                  >
                    {contents}
                  </Text>
                </Box>
              ))}
            </Box>
          ))}
          <Box>
            <Text
              mt={{ base: 14, lg: 30, xl: 30 }}
              c="#FFF"
              fw={700}
              fz={{ base: 18, xs: 20, sm: 22, md: 24, lg: 24, xl: 24 }}
            >
              SECTION 20 - CONTACT INFORMATION
            </Text>
            <Text
              style={{ textAlign: "justify" }}
              mt={{ base: 14, lg: 30, xl: 30 }}
              fz={{ base: 14, xs: 16, sm: 18, md: 18, lg: 18, xl: 18 }}
              fw={400}
            >
              Questions about the Terms of Service should be sent to us at{" "}
              <Anchor
                className={classes.termsAndCondition}
                style={{
                  color: "#3366cc",
                  textDecoration: "none",
                }}
                href="mailto:hello@salesjobverse.com"
              >
                hello@salesjobverse.com
              </Anchor>
            </Text>
          </Box>
        </Box>
      </ScrollArea>
    </Box>
  );
};

export default TermsAndConditionDataPage;
