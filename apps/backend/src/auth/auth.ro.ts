import { GoogleUser } from './entities/google.user.entity';

export class AuthRO {
  accessToken: string;
  constructor(accessToken: string) {
    this.accessToken = accessToken;
  }
}

export class GoogleUserRO {
  firstName: string;
  lastName: string;
  email: string;
  referenceId: string;
  picture: string;

  constructor(googleUser: GoogleUser) {
    this.firstName = googleUser.firstName;
    this.lastName = googleUser.lastName;
    this.email = googleUser.email;
    this.referenceId = googleUser.referenceId;
    this.picture = googleUser.picture;
  }
}
