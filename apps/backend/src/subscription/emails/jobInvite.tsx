import {
  Body,
  Button,
  Container,
  Head,
  Html,
  Img,
  Preview,
  Section,
  Text,
} from '@react-email/components';
import * as React from 'react';

interface propsType {
  name: string;
  applyNowUrl: string;
  companyName: string;
}

const imgURL =
  'https://marketing-jobverse-prod-media.s3.ap-south-1.amazonaws.com/logo.png';

export function inviteEmail({ name, applyNowUrl, companyName }: propsType) {
  return (
    <Html>
      <Head />
      <Preview>Thanks for purchasing JobSeeker Directory.</Preview>
      <Body style={main}>
        <Container style={container}>
          <Img src={imgURL} height="33" alt="Jobverse" />
          <Section>
            <Text style={text}>Hello {name},</Text>
            <Text style={text}>
              Your profile on Sales Jobverse has grabbed some attention!
              <br></br>
              <br />
              <b>{companyName}</b> has viewed your profile and invited you to
              check out their job posting.
            </Text>
            <Text style={text}>
              <br />
              Hit that "Apply Now" button to reciprocate their interest.
            </Text>
            <Button style={button} href={applyNowUrl}>
              Apply Now
            </Button>
            <Text style={text}>
              Here's to kickstarting your next adventure!
            </Text>
            <Text style={text}>
              <br />
              Your Career Companion, <br /> Team Sales Jobverse
            </Text>
          </Section>
        </Container>
      </Body>
    </Html>
  );
}

const main = {
  backgroundColor: '#f6f9fc',
  padding: '10px',
};

const container = {
  backgroundColor: '#ffffff',
  border: '1px solid #f0f0f0',
  padding: '45px',
};

const text = {
  fontSize: '16px',
  fontFamily:
    "'Open Sans', 'HelveticaNeue-Light', 'Helvetica Neue Light', 'Helvetica Neue', Helvetica, Arial, 'Lucida Grande', sans-serif",
  fontWeight: '300',
  color: '#404040',
  lineHeight: '26px',
};

const button = {
  backgroundColor: '#ff761a',
  borderRadius: '4px',
  color: '#000000',
  fontFamily: "'Open Sans', 'Helvetica Neue', Arial",
  fontSize: '15px',
  textDecoration: 'none',
  textAlign: 'center' as const,
  display: 'block',
  width: '210px',
  padding: '14px 7px',
};
