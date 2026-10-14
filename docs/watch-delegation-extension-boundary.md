# Standing delegation storage boundary

`watch_delegation` stores the original verified standing Watch authority. Its
payload and digest belong to the registration and recurring-use repository;
identity merge cannot rewrite or re-sign that authority.

The shared canonical extension validator rejects this exact table identifier.
Generic extension reads and canonical create, replace, or delete effects therefore
cannot treat it as a consumer extension. This check runs before either backend's
extension operation. It adds no schema or alternate state authority.

Names such as `watch_delegation_custom` and `watch_custom` remain eligible user
extension names. Identity merge still separately requires their loaded extension
declarations and applies the existing reference and space checks. The regression
source covers all three effect forms for the private table and these near names.
