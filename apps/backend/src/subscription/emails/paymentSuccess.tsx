import {
  Body,
  Container,
  Head,
  Html,
  Img,
  Preview,
  Section,
  Text,
} from '@react-email/components';
import * as React from 'react';

interface PropsType {
  name: string;
  transactionId: string;
  totalAmount: number;
  paymentDate: Date;
  email: string;
  subscriptionType: 'Paid Job Boost' | 'JobSeeker Directory' | 'Offer Package';
}

const imgURL =
  'https://marketing-jobverse-prod-media.s3.ap-south-1.amazonaws.com/logo.png';

export function paymentSuccessEmail({
  name,
  totalAmount,
  transactionId,
  paymentDate,
  email,
  subscriptionType,
}: PropsType) {
  return (
    <Html>
      <Head />
      <Preview>Thanks for purchasing {subscriptionType} .</Preview>
      <Body style={main}>
        <Container style={container}>
          <Img src={imgURL} height="33" alt="Jobverse" />
          <Section>
            <Text style={text}>Hi {name},</Text>
            <Text style={text}>
              We sincerely appreciate your recent payment on our platform to
              access the {subscriptionType}.
            </Text>
            <Text style={text}>
              Username/Email associated with the account : {email}
            </Text>
            <Text style={text}>Transaction Id : {transactionId}</Text>
            <Text style={text}>
              Date of Payment :{' '}
              {`${paymentDate.getDate().toLocaleString()} ${
                monthMap[paymentDate.getMonth()]
              } ${paymentDate.getFullYear()}`}
            </Text>
            <Text style={text}>Total Amount : ₹{totalAmount}</Text>
          </Section>
        </Container>
      </Body>
    </Html>
  );
}

const monthMap: { [key: number]: string } = {
  0: 'January',
  1: 'February',
  2: 'March',
  3: 'April',
  4: 'May',
  5: 'June',
  6: 'July',
  7: 'August',
  8: 'September',
  9: 'October',
  10: 'November',
  11: 'December',
};

const main = {
  backgroundColor: '#f6f9fc',
  padding: '10px 0',
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
