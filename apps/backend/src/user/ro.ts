import { User } from './entities/user.entity';

export class UserRO {
  id: number;
  firstName: string;
  lastName: string;
  email: string;
  picture: string | null;

  constructor(user: User) {
    this.id = user.id;
    this.firstName = user.firstName;
    this.lastName = user.lastName;
    this.email = user.email;
    this.picture = user.picture;
  }
}
