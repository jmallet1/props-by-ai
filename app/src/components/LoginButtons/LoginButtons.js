import { useAuth } from "react-oidc-context";
import './LoginButtons.css'

function LoginButtons({windowWidth}) {
  let loginText = ""
  if(windowWidth > 768){
    loginText = (<>Login or Sign Up</>);
  } else {
    loginText = (<>Login<div className="loginLineBreak"></div>Sign Up</>)
  }

  

  const auth = useAuth();

  const signOutRedirect = () => {
    const clientId = "5dk9qpkaq65u3uekde46kmsvt1";
    const logoutUri = "<logout uri>";
    const cognitoDomain = "https://us-east-27o6rrv5ex.auth.us-east-2.amazoncognito.com";
    window.location.href = `${cognitoDomain}/logout?client_id=${clientId}&logout_uri=${encodeURIComponent(logoutUri)}`;
  };

  if (auth.isLoading) {
    return <div className="LoginButtonContainer"></div>;
  }

  if (auth.error) {
    return <div className="LoginButtonContainer">Encountering error... {auth.error.message}</div>;
  }

  if (auth.isAuthenticated) {
    return (
      <div className="SignOutButtonContainer">
        {/* <pre> Hello: {auth.user?.profile.email} </pre>
        <pre> ID Token: {auth.user?.id_token} </pre>
        <pre> Access Token: {auth.user?.access_token} </pre>
        <pre> Refresh Token: {auth.user?.refresh_token} </pre> */}

        <button onClick={() => auth.removeUser()}>Sign Out</button>
      </div>
    );
  } else {
    return (
        <div className="LoginButtonContainer">
            <button onClick={() => auth.signinRedirect()}>{loginText}</button>
            {/* <button onClick={() => signOutRedirect()}>Sign out</button> */}
        </div>
    );
  }
}
  
export default LoginButtons;